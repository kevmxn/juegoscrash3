#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor Spaceman con señales cada 1s, grupos de 2 intentos,
estadísticas por tipo, envío en tiempo real y auto‑ping para mantener activo el servicio.
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import logging
import os
import random
from datetime import datetime
from typing import Set, Dict, Any, List
from collections import defaultdict, deque
import aiosqlite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN SPACEMAN
# ============================================
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301
DB_PATH = "spaceman_data.db"

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

# Historial y almacenamiento
spaceman_multipliers = deque(maxlen=100)
spaceman_events_seen: Set[str] = set()
spaceman_history: list = []
MAX_HISTORY = 100
MAX_STORAGE = 100000

current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

connected_clients: Set[web.WebSocketResponse] = set()
event_queue = asyncio.Queue()
BATCH_SIZE = 20
BATCH_TIMEOUT = 1.0

TABLE_UPDATE_MIN = 60
TABLE_UPDATE_MAX = 120

# ============================================
# GESTIÓN DE SEÑALES
# ============================================
class SignalGroup:
    def __init__(self, timestamp: datetime, prediction: str, target_range: str):
        self.id = f"{timestamp.timestamp()}"
        self.start_time = timestamp
        self.prediction = prediction
        self.target_range = target_range
        self.attempts = []
        self.resolved = False

    def add_attempt(self, multiplier: float) -> bool:
        success = (self.prediction == "positive" and multiplier > 1.5) or \
                  (self.prediction == "negative" and multiplier <= 1.5)
        self.attempts.append((multiplier, success))
        if len(self.attempts) == 1 and success:
            return True
        if len(self.attempts) == 2:
            return True
        return False

    def is_successful(self) -> bool:
        return any(s for _, s in self.attempts)

    def get_attempts_info(self):
        return [(m, s) for m, s in self.attempts]

active_groups: List[SignalGroup] = []
signal_history: List[SignalGroup] = []

global_stats = {"total_groups": 0, "successful_groups": 0, "success_rate": 0.0}
type_stats = {
    "positive": {"total": 0, "successful": 0, "rate": 0.0},
    "negative": {"total": 0, "successful": 0, "rate": 0.0}
}

# ============================================
# ANÁLISIS DE TENDENCIA
# ============================================
def calculate_volatility(values):
    if len(values) < 2:
        return 0
    diffs = [abs(values[i] - values[i-1]) for i in range(1, len(values))]
    return sum(diffs) / len(diffs)

def calculate_momentum(values):
    if len(values) < 5:
        return 0
    return sum(values[-5:]) / 5

def detect_pattern(values):
    if len(values) < 5:
        return None
    for length in [2, 3]:
        if len(values) >= length*2:
            last_seq = list(values[-length:])
            prev_seq = list(values[-length*2:-length])
            if last_seq == prev_seq:
                return {"type": "repeat", "length": length}
    return None

def analyze_trend(multipliers):
    if len(multipliers) < 10:
        return None, None
    last10 = list(multipliers)[-10:]
    vol = calculate_volatility(last10)
    mom = calculate_momentum(last10)
    pat = detect_pattern(last10)
    if vol > 0.5 and mom < -0.3:
        return "positive", "🎯 1.50x - 1.90x"
    if vol < 0.3 and mom > 0.3:
        return "positive", "✨ 1.40x - 1.80x"
    if pat:
        return "positive", "🔁 Patrón detectado"
    return None, None

# ============================================
# SOPORTE / RESISTENCIA
# ============================================
class SupportResistanceDetector:
    def __init__(self, window=20):
        self.window = window
        self.support = None
        self.resistance = None
        self.support_touches = 0
        self.resistance_touches = 0

    def update(self, values):
        if len(values) < self.window:
            return []
        recent = list(values)[-self.window:]
        min_val, max_val = min(recent), max(recent)
        alerts = []
        # Soporte
        if abs(recent[-1] - min_val) < 0.05 * min_val:
            if self.support != min_val:
                self.support = min_val
                self.support_touches = 1
            else:
                self.support_touches += 1
                if self.support_touches == 2:
                    alerts.append("🎯 Posible 1,50x - 2,30x")
                elif self.support_touches >= 3:
                    alerts.append("⚠️ Esperar")
                    self.support = None
                    self.support_touches = 0
        # Resistencia
        if abs(recent[-1] - max_val) < 0.05 * max_val:
            if self.resistance != max_val:
                self.resistance = max_val
                self.resistance_touches = 1
            else:
                self.resistance_touches += 1
                if self.resistance_touches == 2:
                    alerts.append("⚠️ Esperar")
                elif self.resistance_touches >= 3:
                    self.resistance = None
                    self.resistance_touches = 0
        return alerts

sr_detector = SupportResistanceDetector()

# ============================================
# GENERACIÓN DE SEÑALES (cada 1s)
# ============================================
async def signal_generator():
    while True:
        await asyncio.sleep(1.0)
        if not spaceman_multipliers or len(spaceman_multipliers) < 10:
            continue
        mult_list = list(spaceman_multipliers)
        pred, rng = analyze_trend(mult_list)
        if pred:
            group = SignalGroup(datetime.now(), pred, rng)
            active_groups.append(group)
            await broadcast({
                'tipo': 'señal',
                'prediccion': pred,
                'rango': rng,
                'timestamp': group.start_time.isoformat()
            })
            logger.info(f"📡 Señal: {pred} - {rng}")
        alerts = sr_detector.update(mult_list)
        for alert in alerts:
            await broadcast({
                'tipo': 'alerta',
                'mensaje': alert,
                'timestamp': datetime.now().isoformat()
            })
            logger.info(f"⚠️ Alerta: {alert}")

# ============================================
# ACTUALIZACIÓN DE ESTADÍSTICAS
# ============================================
def update_stats(group: SignalGroup):
    success = group.is_successful()
    pred = group.prediction
    global_stats['total_groups'] += 1
    if success:
        global_stats['successful_groups'] += 1
    global_stats['success_rate'] = global_stats['successful_groups'] / global_stats['total_groups'] if global_stats['total_groups'] else 0.0
    type_stats[pred]['total'] += 1
    if success:
        type_stats[pred]['successful'] += 1
    type_stats[pred]['rate'] = type_stats[pred]['successful'] / type_stats[pred]['total'] if type_stats[pred]['total'] else 0.0

# ============================================
# PROCESAMIENTO DE NUEVO MULTIPLICADOR
# ============================================
async def process_new_multiplier(event: dict):
    mult = event['maxMultiplier']
    spaceman_multipliers.append(mult)

    to_remove = []
    for grp in active_groups:
        if grp.resolved:
            continue
        should_resolve = grp.add_attempt(mult)
        if should_resolve:
            to_remove.append(grp)
            if len(grp.attempts) == 1 and not grp.attempts[0][1]:
                await broadcast({
                    'tipo': 'segundo_intento',
                    'grupo_id': grp.id,
                    'prediccion': grp.prediction,
                    'rango': grp.target_range,
                    'primer_intento': grp.attempts[0][0]
                })
                logger.info(f"⚠️ Primer intento fallido, grupo {grp.id}")
    for grp in to_remove:
        if grp.resolved:
            continue
        grp.resolved = True
        active_groups.remove(grp)
        success = grp.is_successful()
        update_stats(grp)
        signal_history.append(grp)
        if len(signal_history) > 100:
            signal_history.pop(0)
        await broadcast({
            'tipo': 'resultado_senal',
            'grupo_id': grp.id,
            'prediccion': grp.prediction,
            'rango': grp.target_range,
            'intentos': grp.get_attempts_info(),
            'exito': success,
            'estadisticas_globales': global_stats,
            'estadisticas_por_tipo': type_stats,
            'tabla_niveles': {k: dict(v) for k, v in level_counts.items()},
            'nivel_actual': current_level
        })
        logger.info(f"✅ Señal resuelta: grupo {grp.id} | Éxito: {success}")
    await event_queue.put(event)

# ============================================
# BROADCAST
# ============================================
async def broadcast(msg):
    if not connected_clients:
        return
    message = json.dumps(msg, default=str)
    await asyncio.gather(*[c.send_str(message) for c in connected_clients], return_exceptions=True)

# ============================================
# MONITOREO SPACEMAN (versión original funcional)
# ============================================
async def monitor_spaceman():
    global current_level, spaceman_history, level_counts
    reconnect_delay = BASE_RECONNECT_DELAY
    logger.info("[SPACEMAN] 🚀 Iniciando monitor")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    logger.info("[SPACEMAN] ✅ WebSocket conectado")
                    await ws.send_json({
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    })
                    logger.info("[SPACEMAN] 📡 Suscripción enviada")
                    reconnect_delay = BASE_RECONNECT_DELAY
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                if "gameResult" in data and data["gameResult"]:
                                    result_data = data["gameResult"][0]
                                    result_str = result_data.get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00:
                                            game_id = result_data.get("gameId") or result_data.get("roundId") or result_data.get("id")
                                            if not game_id:
                                                game_id = f"spaceman_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{multiplier:.2f}"
                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)
                                                if multiplier < 2.00:
                                                    current_level -= 1
                                                else:
                                                    current_level += 1
                                                range_key = None
                                                if 3.00 <= multiplier <= 4.99:
                                                    range_key = '3-4.99'
                                                elif 5.00 <= multiplier <= 9.99:
                                                    range_key = '5-9.99'
                                                elif multiplier >= 10.00:
                                                    range_key = '10+'
                                                evento = {
                                                    'tipo': 'spaceman',
                                                    'event_id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'timestamp_recepcion': datetime.now().isoformat(),
                                                    'nivel': current_level
                                                }
                                                spaceman_history.insert(0, evento)
                                                if len(spaceman_history) > MAX_HISTORY:
                                                    spaceman_history.pop()
                                                if range_key:
                                                    level_counts[current_level][range_key] += 1
                                                await save_event(evento)
                                                if range_key:
                                                    await update_count(current_level, range_key)
                                                await update_current_level(current_level)
                                                await process_new_multiplier(evento)
                                                logger.info(f"[SPACEMAN] 🚀 {multiplier:.2f}x | Nivel={current_level}")
                            except Exception as e:
                                logger.debug(f"Error procesando: {e}")
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.info("[SPACEMAN] 🔌 Conexión cerrada")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"[SPACEMAN] ❌ Error: {ws.exception()}")
                            break
        except Exception as e:
            logger.error(f"[SPACEMAN] 💥 {e}, reconexión en {reconnect_delay:.1f}s")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# AUTO‑PING (para evitar que Render duerma el servicio)
# ============================================
async def self_ping():
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)  # cada 10 minutos
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        logger.info("[PING] Auto‑ping exitoso, servicio activo")
                    else:
                        logger.warning(f"[PING] Auto‑ping falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error en auto‑ping: {e}")

# ============================================
# SERVIDOR WEB + WEBSOCKET
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if spaceman_multipliers:
            await ws.send_json({'tipo': 'historial_multipliers', 'multipliers': list(spaceman_multipliers)})
        await ws.send_json({
            'tipo': 'estado',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()},
            'estadisticas_globales': global_stats,
            'estadisticas_por_tipo': type_stats,
            'grupos_activos': [{
                'id': g.id, 'prediccion': g.prediction, 'rango': g.target_range,
                'intentos_realizados': len(g.attempts)
            } for g in active_groups]
        })
        logger.info("Cliente conectado")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    # HTML embebido (monitor minimalista)
    html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VirusBet - Monitor Spaceman</title>
    <style>
        body { background: #0a0a12; color: #e0e0e0; font-family: sans-serif; padding: 20px; }
        .card { background: #111118; border-radius: 16px; padding: 16px; margin-bottom: 16px; border: 1px solid #2a2a35; }
        h1 { color: #ffd966; }
        .stats { display: flex; gap: 16px; flex-wrap: wrap; }
        .stat-box { background: #1e1e2a; padding: 12px; border-radius: 12px; flex: 1; }
        .signal { background: #1e1e2a; border-left: 4px solid #ffd966; padding: 10px; margin: 10px 0; }
        .attempt { border-left: 4px solid #ff9800; padding: 10px; margin: 10px 0; }
        .alert { border-left: 4px solid #ff5722; padding: 8px; margin: 5px 0; font-size: 0.9rem; background: #2a1e2a; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #2a2a35; padding: 6px; text-align: center; }
        .success { color: #4caf50; }
        .fail { color: #f44336; }
        canvas { max-height: 300px; width: 100%; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>🚀 VirusBet · Spaceman Signals</h1>
    <div class="card" id="stats-global">
        <h3>📊 Estadísticas Globales</h3>
        <div>Total señales: <span id="total">0</span></div>
        <div>Exitosas: <span id="success">0</span></div>
        <div>Tasa: <span id="rate">0%</span></div>
        <h3>📈 Por Tipo</h3>
        <div>Positivas: <span id="posTotal">0</span> | Aciertos: <span id="posSucc">0</span> (<span id="posRate">0</span>%)</div>
        <div>Negativas: <span id="negTotal">0</span> | Aciertos: <span id="negSucc">0</span> (<span id="negRate">0</span>%)</div>
    </div>
    <div class="card">
        <h3>🎚️ Nivel actual: <span id="level">0</span></h3>
        <table id="levelTable"><thead><tr><th>Nivel</th><th>3-4.99</th><th>5-9.99</th><th>10+</th></tr></thead><tbody><tr><td colspan="3">Cargando...</td></tr></tbody></table>
    </div>
    <div class="card">
        <h3>🔔 Última señal</h3>
        <div id="lastSignal" class="signal">—</div>
        <h3>🎯 Estado</h3>
        <div id="attemptStatus" class="attempt">Esperando señal...</div>
    </div>
    <div class="card">
        <h3>⚠️ Alertas</h3>
        <div id="alerts"></div>
    </div>
    <div class="card">
        <h3>📜 Historial (últimas 10)</h3>
        <div id="history"></div>
    </div>
    <div class="card">
        <h3>📈 Evolución multiplicadores</h3>
        <canvas id="chart"></canvas>
    </div>
    <script>
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        let chart = null, multipliers = [];

        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            if (d.tipo === 'historial_multipliers') { multipliers = d.multipliers; updateChart(); }
            if (d.tipo === 'estado') {
                if (d.multipliers) { multipliers = d.multipliers; updateChart(); }
                updateLevelTable(d.conteos, d.nivel_actual);
                updateStats(d.estadisticas_globales, d.estadisticas_por_tipo);
            }
            if (d.tipo === 'señal') {
                document.getElementById('lastSignal').innerHTML = `<strong>${d.prediccion==='positive'?'📈 Subida':'📉 Bajada'}</strong><br>Rango: ${d.rango}<br>Hora: ${new Date(d.timestamp).toLocaleTimeString()}`;
                document.getElementById('attemptStatus').innerHTML = 'Esperando primer intento...';
            }
            if (d.tipo === 'segundo_intento') {
                document.getElementById('attemptStatus').innerHTML = `⚠️ Primer intento falló (${d.primer_intento.toFixed(2)}x).<br>Esperando segundo intento...`;
            }
            if (d.tipo === 'resultado_senal') {
                updateStats(d.estadisticas_globales, d.estadisticas_por_tipo);
                updateLevelTable(d.tabla_niveles, d.nivel_actual);
                let intentos = d.intentos.map(([m,s]) => `<div class="${s?'success':'fail'}">Multiplicador: ${m.toFixed(2)}x - ${s?'✅ Acertó':'❌ Falló'}</div>`).join('');
                document.getElementById('attemptStatus').innerHTML = `<strong>Señal ${d.exito?'✅ ACERTADA':'❌ FALLIDA'}</strong><br>${intentos}`;
                addToHistory(d);
            }
            if (d.tipo === 'alerta') {
                const alertDiv = document.createElement('div'); alertDiv.className = 'alert'; alertDiv.innerHTML = `⚠️ ${d.mensaje} — ${new Date(d.timestamp).toLocaleTimeString()}`;
                document.getElementById('alerts').prepend(alertDiv);
                while(document.getElementById('alerts').children.length > 5) document.getElementById('alerts').removeChild(document.getElementById('alerts').lastChild);
            }
        };

        function updateChart() {
            const ctx = document.getElementById('chart').getContext('2d');
            if (chart) chart.destroy();
            chart = new Chart(ctx, { type: 'line', data: { labels: multipliers.map((_,i)=>i+1), datasets: [{ label: 'Multiplicador', data: multipliers, borderColor: '#ffd966', tension: 0.2 }] }, options: { responsive: true } });
        }
        function updateLevelTable(levels, currentLvl) {
            let html = '<thead><tr><th>Nivel</th><th>3-4.99</th><th>5-9.99</th><th>10+</th></tr></thead><tbody>';
            for (let lvl of Object.keys(levels).sort((a,b)=>b-a)) {
                html += `<tr style="${lvl==currentLvl?'background:#ffd96620;':''}"><td>${lvl}</td><td>${levels[lvl]['3-4.99']||0}</td><td>${levels[lvl]['5-9.99']||0}</td><td>${levels[lvl]['10+']||0}</td></tr>`;
            }
            html += '</tbody>';
            document.getElementById('levelTable').innerHTML = html;
            document.getElementById('level').innerText = currentLvl;
        }
        function updateStats(g, t) {
            if(g) { document.getElementById('total').innerText = g.total_groups; document.getElementById('success').innerText = g.successful_groups; document.getElementById('rate').innerText = `${(g.success_rate*100).toFixed(1)}%`; }
            if(t) { document.getElementById('posTotal').innerText = t.positive.total; document.getElementById('posSucc').innerText = t.positive.successful; document.getElementById('posRate').innerText = (t.positive.rate*100).toFixed(1); document.getElementById('negTotal').innerText = t.negative.total; document.getElementById('negSucc').innerText = t.negative.successful; document.getElementById('negRate').innerText = (t.negative.rate*100).toFixed(1); }
        }
        function addToHistory(s) {
            const histDiv = document.getElementById('history');
            const item = document.createElement('div'); item.className = 'alert'; item.style.borderLeftColor = s.exito ? '#4caf50' : '#f44336';
            item.innerHTML = `${new Date().toLocaleTimeString()} — ${s.prediccion==='positive'?'📈':'📉'} ${s.rango} — ${s.exito?'✅ ACERTADA':'❌ FALLIDA'}`;
            histDiv.prepend(item);
            while(histDiv.children.length > 10) histDiv.removeChild(histDiv.lastChild);
        }
    </script>
</body>
</html>"""
    return web.Response(text=html, content_type='text/html')

# ============================================
# FUNCIONES DE BASE DE DATOS (simplificadas)
# ============================================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, maxMultiplier REAL, timestamp_recepcion TEXT, nivel INTEGER)')
        await db.execute('CREATE TABLE IF NOT EXISTS counts (level INTEGER, range TEXT, count INTEGER, PRIMARY KEY (level, range))')
        await db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)')
        await db.execute('CREATE TABLE IF NOT EXISTS signal_stats (total_groups INTEGER, successful_groups INTEGER, positive_total INTEGER, positive_successful INTEGER, negative_total INTEGER, negative_successful INTEGER)')
        await db.commit()

async def save_event(event: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO events (id, maxMultiplier, timestamp_recepcion, nivel) VALUES (?, ?, ?, ?)',
                         (event['event_id'], event['maxMultiplier'], event['timestamp_recepcion'], event['nivel']))
        await db.execute('DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY timestamp_recepcion DESC LIMIT ?)', (MAX_STORAGE,))
        await db.commit()

async def update_count(level: int, range_key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO counts (level, range, count) VALUES (?, ?, 1) ON CONFLICT(level, range) DO UPDATE SET count = count + 1',
                         (level, range_key))
        await db.commit()

async def update_current_level(level: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)', ('current_level', str(level)))
        await db.commit()

# ============================================
# SERVIDOR WEB
# ============================================
async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', root_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"✅ Servidor HTTP/WS escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor Spaceman con señales, auto‑ping y monitor web")
    logger.info("=" * 60)
    await init_db()
    # Cargar estadísticas previas (opcional)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT total_groups, successful_groups, positive_total, positive_successful, negative_total, negative_successful FROM signal_stats') as cursor:
            row = await cursor.fetchone()
            if row:
                global_stats['total_groups'] = row[0]
                global_stats['successful_groups'] = row[1]
                global_stats['success_rate'] = global_stats['successful_groups'] / global_stats['total_groups'] if global_stats['total_groups'] else 0.0
                type_stats['positive']['total'] = row[2]
                type_stats['positive']['successful'] = row[3]
                type_stats['positive']['rate'] = type_stats['positive']['successful'] / type_stats['positive']['total'] if type_stats['positive']['total'] else 0.0
                type_stats['negative']['total'] = row[4]
                type_stats['negative']['successful'] = row[5]
                type_stats['negative']['rate'] = type_stats['negative']['successful'] / type_stats['negative']['total'] if type_stats['negative']['total'] else 0.0
    asyncio.create_task(signal_generator())
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_spaceman()),
        asyncio.create_task(self_ping()),  # ¡Auto‑ping activado!
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
