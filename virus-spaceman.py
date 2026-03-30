#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor Spaceman con generación de señales cada 1s,
grupos de dos intentos con cierre inmediato en acierto,
estadísticas globales y por tipo de señal,
y envío de tablas de niveles al resolver cada señal.
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
from typing import Set, Dict, Any, List, Optional
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
spaceman_multipliers = deque(maxlen=100)      # Últimos 100 multiplicadores
spaceman_events_seen = set()
MAX_STORAGE = 100000

# Niveles y conteos
current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

# Clientes WebSocket
connected_clients: Set[web.WebSocketResponse] = set()

# Batching
event_queue = asyncio.Queue()
BATCH_SIZE = 20
BATCH_TIMEOUT = 1.0

TABLE_UPDATE_MIN = 60
TABLE_UPDATE_MAX = 120

# ============================================
# GESTIÓN DE SEÑALES Y ESTADÍSTICAS
# ============================================
class SignalGroup:
    def __init__(self, timestamp: datetime, prediction: str, target_range: str):
        self.id = f"{timestamp.timestamp()}"
        self.start_time = timestamp
        self.prediction = prediction        # "positive" o "negative"
        self.target_range = target_range    # string como "1.50x-1.90x"
        self.attempts = []                  # lista de (multiplier, success)
        self.resolved = False

    def add_attempt(self, multiplier: float) -> bool:
        """
        Añade un intento. Devuelve True si el grupo debe resolverse inmediatamente.
        Condiciones:
        - Si este es el primer intento y es exitoso -> resolver ahora.
        - Si este es el segundo intento (ya había un intento previo) -> resolver ahora.
        """
        success = False
        if self.prediction == "positive" and multiplier > 1.5:
            success = True
        elif self.prediction == "negative" and multiplier <= 1.5:
            success = True
        self.attempts.append((multiplier, success))

        # Resolver si:
        # - Es el primer intento y acertó
        if len(self.attempts) == 1 and success:
            return True
        # - Ya tenemos dos intentos (sea cual sea el resultado)
        if len(self.attempts) == 2:
            return True
        return False

    def is_successful(self) -> bool:
        """Devuelve True si al menos un intento fue exitoso."""
        return any(success for _, success in self.attempts)

    def get_attempts_info(self):
        """Devuelve lista de intentos en formato serializable."""
        return [(m, s) for m, s in self.attempts]


active_groups: List[SignalGroup] = []        # Grupos activos (esperando intentos)
signal_history: List[SignalGroup] = []       # Grupos resueltos (hasta 100)

# Estadísticas globales
global_stats = {
    "total_groups": 0,
    "successful_groups": 0,
    "success_rate": 0.0
}

# Estadísticas por tipo de señal
type_stats = {
    "positive": {"total": 0, "successful": 0, "rate": 0.0},
    "negative": {"total": 0, "successful": 0, "rate": 0.0}
}

# ============================================
# ANÁLISIS DE TENDENCIA (igual que en el HTML)
# ============================================
def calculate_volatility(values):
    if len(values) < 2:
        return 0
    diffs = [abs(values[i] - values[i-1]) for i in range(1, len(values))]
    return sum(diffs) / len(diffs)

def calculate_momentum(values):
    if len(values) < 5:
        return 0
    recent = values[-5:]
    return sum(recent) / 5

def detect_pattern(values):
    if len(values) < 5:
        return None
    # Busca secuencias repetidas de longitud 2 o 3
    for length in [2, 3]:
        if len(values) >= length*2:
            last_seq = list(values[-length:])
            prev_seq = list(values[-length*2:-length])
            if last_seq == prev_seq:
                return {"type": "repeat", "length": length, "sequence": last_seq}
    return None

def analyze_trend(multipliers):
    """Devuelve (prediction, range_string) o (None, None) si no hay señal."""
    if len(multipliers) < 10:
        return None, None

    last10 = list(multipliers)[-10:]
    volatility = calculate_volatility(last10)
    momentum = calculate_momentum(last10)
    pattern = detect_pattern(last10)

    # Lógica adaptada del HTML
    if volatility > 0.5 and momentum < -0.3:
        return "positive", "🎯 1.50x - 1.90x"
    if volatility < 0.3 and momentum > 0.3:
        return "positive", "✨ 1.40x - 1.80x"
    # Si hay patrón repetido, también señal positiva
    if pattern:
        return "positive", "🔁 Patrón detectado"

    return None, None

# ============================================
# SOPORTE / RESISTENCIA (simulado)
# ============================================
class SupportResistanceDetector:
    def __init__(self, window=20):
        self.window = window
        self.support = None
        self.resistance = None
        self.support_touches = 0
        self.resistance_touches = 0
        self.last_alert = None

    def update(self, values):
        if len(values) < self.window:
            return None
        recent = list(values)[-self.window:]
        min_val = min(recent)
        max_val = max(recent)

        alerts = []
        # Soporte: si el último valor es igual o muy cercano al mínimo
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
        # Resistencia: si el último valor es igual o muy cercano al máximo
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
# TAREA DE GENERACIÓN DE SEÑALES (cada 1 segundo)
# ============================================
async def signal_generator():
    """Analiza los últimos multiplicadores cada segundo y envía señales."""
    while True:
        await asyncio.sleep(1.0)
        if not spaceman_multipliers or len(spaceman_multipliers) < 10:
            continue

        # Convertir a lista para análisis
        multipliers_list = list(spaceman_multipliers)
        pred, range_str = analyze_trend(multipliers_list)

        # Enviar señal si se detecta una
        if pred:
            # Crear nuevo grupo de señal
            new_group = SignalGroup(datetime.now(), pred, range_str)
            active_groups.append(new_group)

            # Mensaje para clientes
            signal_msg = {
                'tipo': 'señal',
                'prediccion': pred,
                'rango': range_str,
                'timestamp': new_group.start_time.isoformat()
            }
            await broadcast(signal_msg)
            logger.info(f"📡 Señal enviada: {pred} - {range_str}")

        # También enviar alertas de soporte/resistencia si las hay
        alerts = sr_detector.update(multipliers_list)
        for alert in alerts:
            alert_msg = {
                'tipo': 'alerta',
                'mensaje': alert,
                'timestamp': datetime.now().isoformat()
            }
            await broadcast(alert_msg)
            logger.info(f"⚠️ Alerta: {alert}")

# ============================================
# FUNCIONES DE ACTUALIZACIÓN DE ESTADÍSTICAS
# ============================================
def update_stats(group: SignalGroup):
    """Actualiza estadísticas globales y por tipo con el grupo resuelto."""
    success = group.is_successful()
    pred_type = group.prediction

    # Globales
    global_stats['total_groups'] += 1
    if success:
        global_stats['successful_groups'] += 1
    global_stats['success_rate'] = global_stats['successful_groups'] / global_stats['total_groups'] if global_stats['total_groups'] > 0 else 0.0

    # Por tipo
    type_stats[pred_type]['total'] += 1
    if success:
        type_stats[pred_type]['successful'] += 1
    type_stats[pred_type]['rate'] = type_stats[pred_type]['successful'] / type_stats[pred_type]['total'] if type_stats[pred_type]['total'] > 0 else 0.0

    # Guardar en base de datos (opcional)
    # Aquí podríamos guardar las estadísticas en SQLite para persistencia

# ============================================
# PROCESAMIENTO DE NUEVOS MULTIPLICADORES
# ============================================
async def process_new_multiplier(event: dict):
    """Actualiza grupos activos y resuelve señales."""
    multiplier = event['maxMultiplier']
    # Añadir a historial de análisis
    spaceman_multipliers.append(multiplier)

    # Procesar grupos activos en orden
    groups_to_remove = []
    for group in active_groups:
        if group.resolved:
            continue
        should_resolve = group.add_attempt(multiplier)
        if should_resolve:
            groups_to_remove.append(group)
            # Si es el primer intento y fue exitoso, se resuelve ya
            # Si es el segundo intento (cualquier resultado), también se resuelve
            # Notificar si fue el primer intento y falló (queda segundo)
            if len(group.attempts) == 1 and not group.attempts[0][1]:
                # Primer intento fallido, avisar a clientes que queda segundo intento
                await broadcast({
                    'tipo': 'segundo_intento',
                    'grupo_id': group.id,
                    'prediccion': group.prediction,
                    'rango': group.target_range,
                    'primer_intento': group.attempts[0][0]
                })
                logger.info(f"⚠️ Primer intento fallido para grupo {group.id}, esperando segundo...")
            # Si ya tiene dos intentos, se resolverá después

    # Resolver grupos completados
    for group in groups_to_remove:
        if group.resolved:
            continue
        group.resolved = True
        active_groups.remove(group)
        success = group.is_successful()
        update_stats(group)

        # Guardar en historial (hasta 100)
        signal_history.append(group)
        if len(signal_history) > 100:
            signal_history.pop(0)

        # Preparar mensaje de resultado con tablas actualizadas
        result_msg = {
            'tipo': 'resultado_senal',
            'grupo_id': group.id,
            'prediccion': group.prediction,
            'rango': group.target_range,
            'intentos': group.get_attempts_info(),
            'exito': success,
            'estadisticas_globales': global_stats,
            'estadisticas_por_tipo': type_stats,
            'tabla_niveles': {k: dict(v) for k, v in level_counts.items()}
        }
        await broadcast(result_msg)
        logger.info(f"✅ Señal resuelta: grupo {group.id} | Éxito: {success}")

    # Guardar evento original (como ya estaba)
    await event_queue.put(event)

# ============================================
# FUNCIONES DE BROADCAST
# ============================================
async def broadcast(msg):
    if not connected_clients:
        return
    message = json.dumps(msg, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

# ============================================
# MONITOREO SPACEMAN (igual que antes, pero llama a process_new_multiplier)
# ============================================
async def monitor_spaceman():
    global current_level, spaceman_multipliers, level_counts, _last_gen_time, _gen_counter
    reconnect_delay = BASE_RECONNECT_DELAY
    logger.info("[SPACEMAN] 🚀 Iniciando monitor")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    logger.info("[SPACEMAN] ✅ WebSocket conectado")
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    logger.info("[SPACEMAN] 📡 Suscripción enviada")
                    reconnect_delay = BASE_RECONNECT_DELAY

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                if "gameResult" in data and data["gameResult"]:
                                    result_str = data["gameResult"][0].get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00:
                                            # Generar ID único si no viene
                                            game_id = None
                                            if "gameId" in data:
                                                game_id = data["gameId"]
                                            elif "roundId" in data:
                                                game_id = data["roundId"]
                                            elif "id" in data:
                                                game_id = data["id"]
                                            else:
                                                # generar ID local
                                                now = time.time()
                                                if now == _last_gen_time:
                                                    _gen_counter += 1
                                                else:
                                                    _last_gen_time = now
                                                    _gen_counter = 0
                                                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                                                game_id = f"spaceman_{ts_str}_{_gen_counter}_{multiplier:.2f}"

                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)

                                                # Actualizar nivel
                                                if multiplier < 2.00:
                                                    current_level -= 1
                                                else:
                                                    current_level += 1

                                                # Rango para conteo
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

                                                # Guardar en base de datos y memoria
                                                await save_event(evento)
                                                if range_key:
                                                    level_counts[current_level][range_key] += 1
                                                    await update_count(current_level, range_key)
                                                await update_current_level(current_level)

                                                # Procesar para señales
                                                await process_new_multiplier(evento)

                                                logger.info(f"[SPACEMAN] 🚀 NUEVO: {multiplier:.2f}x | Nivel={current_level}")
                                            else:
                                                logger.info(f"[SPACEMAN] ⚠️ Duplicado: {multiplier:.2f}x")
                            except Exception as e:
                                logger.debug(f"[SPACEMAN] Error procesando mensaje: {e}")
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
# SERVIDOR HTTP + WEBSOCKET (igual que antes, añadimos envio de estadísticas)
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        # Enviar historial de multiplicadores (últimos 100)
        if spaceman_multipliers:
            await ws.send_json({
                'tipo': 'historial_multipliers',
                'multipliers': list(spaceman_multipliers)
            })
        # Enviar estado actual
        await ws.send_json({
            'tipo': 'estado',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()},
            'estadisticas_globales': global_stats,
            'estadisticas_por_tipo': type_stats,
            'grupos_activos': [{
                'id': g.id,
                'prediccion': g.prediction,
                'rango': g.target_range,
                'intentos_realizados': len(g.attempts)
            } for g in active_groups]
        })
        logger.info("Cliente Spaceman conectado, datos iniciales enviados")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    # Servir el HTML incrustado (versión mejorada que muestra estadísticas)
    html = """<!DOCTYPE html>
<html>
<head>
    <title>VirusBet - Señales Spaceman</title>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; background: #0f0f1a; color: #e2e2e2; padding: 20px; }
        #chart { width: 100%; height: 400px; margin-bottom: 20px; }
        .stats { background: #1a1a2e; padding: 15px; border-radius: 8px; margin-bottom: 10px; display: inline-block; width: 45%; vertical-align: top; margin-right: 2%; }
        .signal { background: #16213e; padding: 10px; border-radius: 8px; margin: 10px 0; }
        .attempt { margin: 5px 0; padding: 5px; background: #0f0f1a; border-radius: 4px; }
        .success { color: #0f0; }
        .fail { color: #f00; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>VirusBet - Señales en Tiempo Real</h1>
    <div class="stats">
        <h3>Estadísticas Globales</h3>
        <div>Total señales: <span id="total">0</span></div>
        <div>Señales exitosas: <span id="success">0</span></div>
        <div>Tasa de acierto: <span id="rate">0%</span></div>
    </div>
    <div class="stats">
        <h3>Estadísticas por Tipo</h3>
        <div>Positivas: <span id="pos_total">0</span> | Aciertos: <span id="pos_success">0</span> | Tasa: <span id="pos_rate">0%</span></div>
        <div>Negativas: <span id="neg_total">0</span> | Aciertos: <span id="neg_success">0</span> | Tasa: <span id="neg_rate">0%</span></div>
    </div>
    <div id="signal-area" class="signal">
        <h3>Última señal</h3>
        <div id="last-signal">Esperando...</div>
    </div>
    <div id="attempt-info" class="signal">
        <h3>Intento actual</h3>
        <div id="attempt-status">-</div>
    </div>
    <canvas id="chart"></canvas>
    <script>
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        let chart = null;
        let multipliers = [];

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.tipo === 'historial_multipliers') {
                multipliers = data.multipliers;
                updateChart();
            } else if (data.tipo === 'señal') {
                document.getElementById('last-signal').innerHTML = `
                    <strong>${data.prediccion === 'positive' ? '📈 Subida' : '📉 Bajada'}</strong><br>
                    Rango: ${data.rango}<br>
                    Hora: ${new Date(data.timestamp).toLocaleTimeString()}
                `;
                document.getElementById('attempt-status').innerHTML = 'Esperando primer intento...';
            } else if (data.tipo === 'segundo_intento') {
                document.getElementById('attempt-status').innerHTML = `
                    ⚠️ Primer intento falló (${data.primer_intento.toFixed(2)}x).<br>
                    Esperando segundo intento...
                `;
            } else if (data.tipo === 'resultado_senal') {
                // Actualizar estadísticas
                const g = data.estadisticas_globales;
                const t = data.estadisticas_por_tipo;
                document.getElementById('total').innerText = g.total_groups;
                document.getElementById('success').innerText = g.successful_groups;
                document.getElementById('rate').innerText = `${(g.success_rate * 100).toFixed(1)}%`;
                document.getElementById('pos_total').innerText = t.positive.total;
                document.getElementById('pos_success').innerText = t.positive.successful;
                document.getElementById('pos_rate').innerText = `${(t.positive.rate * 100).toFixed(1)}%`;
                document.getElementById('neg_total').innerText = t.negative.total;
                document.getElementById('neg_success').innerText = t.negative.successful;
                document.getElementById('neg_rate').innerText = `${(t.negative.rate * 100).toFixed(1)}%`;

                // Mostrar resultado de la señal
                const intentosHtml = data.intentos.map(([m, s]) => `<div class="attempt ${s ? 'success' : 'fail'}">Multiplicador: ${m.toFixed(2)}x - ${s ? '✅ Acertó' : '❌ Falló'}</div>`).join('');
                document.getElementById('attempt-status').innerHTML = `
                    <strong>Señal ${data.exito ? '✅ ACERTADA' : '❌ FALLIDA'}</strong><br>
                    ${intentosHtml}
                `;

                // Opcional: mostrar tabla de niveles
                console.log('Tabla de niveles actualizada:', data.tabla_niveles);
            }
        };

        function updateChart() {
            const ctx = document.getElementById('chart').getContext('2d');
            if (chart) chart.destroy();
            chart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: multipliers.map((_, i) => i+1),
                    datasets: [{
                        label: 'Multiplicador',
                        data: multipliers,
                        borderColor: 'cyan',
                        tension: 0.1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false
                }
            });
        }
    </script>
</body>
</html>"""
    return web.Response(text=html, content_type='text/html')

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
    logger.info(f"✅ Servidor escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# FUNCIONES DE BASE DE DATOS (sin cambios)
# ============================================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                maxMultiplier REAL,
                timestamp_recepcion TEXT,
                nivel INTEGER
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS counts (
                level INTEGER,
                range TEXT,
                count INTEGER,
                PRIMARY KEY (level, range)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS signal_stats (
                total_groups INTEGER,
                successful_groups INTEGER,
                positive_total INTEGER,
                positive_successful INTEGER,
                negative_total INTEGER,
                negative_successful INTEGER
            )
        ''')
        await db.commit()

async def save_event(event: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR REPLACE INTO events (id, maxMultiplier, timestamp_recepcion, nivel)
            VALUES (?, ?, ?, ?)
        ''', (event['event_id'], event['maxMultiplier'], event['timestamp_recepcion'], event['nivel']))
        await db.execute('''
            DELETE FROM events WHERE id NOT IN (
                SELECT id FROM events ORDER BY timestamp_recepcion DESC LIMIT ?
            )
        ''', (MAX_STORAGE,))
        await db.commit()

async def update_count(level: int, range_key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO counts (level, range, count) VALUES (?, ?, 1)
            ON CONFLICT(level, range) DO UPDATE SET count = count + 1
        ''', (level, range_key))
        await db.commit()

async def update_current_level(level: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
        ''', ('current_level', str(level)))
        await db.commit()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor Spaceman con señales cada 1s, grupos de 2 intentos, estadísticas por tipo")
    logger.info("=" * 60)
    await init_db()
    # Cargar estadísticas si existen
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT total_groups, successful_groups, positive_total, positive_successful, negative_total, negative_successful FROM signal_stats') as cursor:
            row = await cursor.fetchone()
            if row:
                global_stats['total_groups'] = row[0]
                global_stats['successful_groups'] = row[1]
                global_stats['success_rate'] = global_stats['successful_groups'] / global_stats['total_groups'] if global_stats['total_groups'] > 0 else 0.0
                type_stats['positive']['total'] = row[2]
                type_stats['positive']['successful'] = row[3]
                type_stats['positive']['rate'] = type_stats['positive']['successful'] / type_stats['positive']['total'] if type_stats['positive']['total'] > 0 else 0.0
                type_stats['negative']['total'] = row[4]
                type_stats['negative']['successful'] = row[5]
                type_stats['negative']['rate'] = type_stats['negative']['successful'] / type_stats['negative']['total'] if type_stats['negative']['total'] > 0 else 0.0
    asyncio.create_task(signal_generator())
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_spaceman()),
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
