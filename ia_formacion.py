"""
ia_formacion.py
===============
Módulo de Inteligencia Artificial para el sistema de Formación.
Usa Groq (gratuito) para:

  1. Analizar progreso de un alumno y dar recomendaciones
  2. Predecir riesgo de no aprobar (🔴 Alto / 🟡 Medio / 🟢 Bajo)
  3. Generar mensajes WhatsApp personalizados
  4. Chatbot del tutor con acceso a datos reales de la BD

CONFIGURACIÓN:
  Añade en el archivo .env de la raíz del proyecto:
      GROQ_API_KEY=tu_clave_aqui

  Obtén tu clave gratis en: https://console.groq.com
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date
from typing import Any

from groq import Groq
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "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")
FORM_DB      = "formacion.db"
MODEL_NAME   = "llama-3.3-70b-versatile"  # modelo gratuito, muy potente en español

_client: Groq | None = None


def _get_client() -> Groq:
    """Inicializa el cliente Groq una sola vez (singleton)."""
    global _client
    if _client is None:
        if not GROQ_API_KEY:
            raise ValueError(
                "GROQ_API_KEY no configurada. "
                "Añade GROQ_API_KEY=tu_clave en el archivo .env"
            )
        _client = Groq(api_key=GROQ_API_KEY)
    return _client


def _llamar_groq(prompt: str, max_tokens: int = 800) -> str:
    """Llama a Groq y devuelve el texto de respuesta."""
    try:
        client   = _get_client()
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error al contactar con la IA: {str(e)}"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE BD
# ─────────────────────────────────────────────────────────────────────────────

def _get_alumno(alumno_id: int) -> dict | None:
    conn   = sqlite3.connect(FORM_DB)
    conn.row_factory = sqlite3.Row
    row    = conn.execute("SELECT * FROM alumnos WHERE id=?", (alumno_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _get_alumnos_tutor(tutor_id: int) -> list[dict]:
    conn = sqlite3.connect(FORM_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM alumnos WHERE tutor_id=? AND (archivado IS NULL OR archivado=0) "
        "ORDER BY progreso ASC",
        (tutor_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_historial(alumno_id: int) -> list[dict]:
    conn = sqlite3.connect(FORM_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT fecha_import, progreso, examenes, delta_progreso "
        "FROM progreso_historial WHERE alumno_id=? ORDER BY fecha_import ASC",
        (alumno_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _resumen_tutor(tutor_id: int) -> dict:
    """Estadísticas globales del tutor para el chatbot."""
    alumnos = _get_alumnos_tutor(tutor_id)
    if not alumnos:
        return {}

    total      = len(alumnos)
    superan    = sum(1 for a in alumnos if a.get("supera_75"))
    sin_tel    = sum(1 for a in alumnos if not a.get("telefono"))
    hoy        = date.today()

    vencidos   = []
    en_riesgo  = []
    por_vencer = []

    for a in alumnos:
        progreso  = float(a.get("progreso") or 0)
        fecha_fin = a.get("fecha_fin")
        supera    = a.get("supera_75")
        nombre    = a.get("nombre", "")
        curso     = a.get("curso", "")

        if fecha_fin:
            try:
                dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            except Exception:
                dias = None
        else:
            dias = None

        if dias is not None and dias < 0 and not supera:
            vencidos.append({"nombre": nombre, "curso": curso, "progreso": progreso})
        elif dias is not None and 0 <= dias <= 14 and progreso < 50 and not supera:
            en_riesgo.append({"nombre": nombre, "curso": curso, "progreso": progreso, "dias": dias})
        elif dias is not None and 0 <= dias <= 30 and not supera:
            por_vencer.append({"nombre": nombre, "curso": curso, "progreso": progreso, "dias": dias})

    cursos = {}
    for a in alumnos:
        c = a.get("curso") or "Sin curso"
        cursos.setdefault(c, {"total": 0, "superan": 0, "prog_sum": 0})
        cursos[c]["total"]    += 1
        cursos[c]["superan"]  += int(a.get("supera_75") or 0)
        cursos[c]["prog_sum"] += float(a.get("progreso") or 0)

    resumen_cursos = {
        c: {
            "total":   v["total"],
            "superan": v["superan"],
            "avg_prog": round(v["prog_sum"] / v["total"], 1) if v["total"] else 0,
        }
        for c, v in cursos.items()
    }

    return {
        "total_alumnos": total,
        "superan_75":    superan,
        "pct_exito":     round(superan / total * 100, 1) if total else 0,
        "sin_telefono":  sin_tel,
        "vencidos":      vencidos,
        "en_riesgo":     en_riesgo,
        "por_vencer":    por_vencer,
        "cursos":        resumen_cursos,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. ANALIZAR ALUMNO
# ─────────────────────────────────────────────────────────────────────────────

def analizar_alumno(alumno_id: int) -> dict:
    """
    Analiza el progreso de un alumno y devuelve:
      - diagnostico     : texto con análisis completo
      - riesgo          : "alto" | "medio" | "bajo"
      - riesgo_emoji    : 🔴 | 🟡 | 🟢
      - recomendaciones : lista de acciones concretas
      - mensaje_wa      : mensaje WhatsApp personalizado listo para enviar
    """
    alumno = _get_alumno(alumno_id)
    if not alumno:
        return {"error": "Alumno no encontrado"}

    historial = _get_historial(alumno_id)
    hoy       = date.today()

    nombre    = alumno.get("nombre", "")
    curso     = alumno.get("curso") or "Sin curso"
    progreso  = float(alumno.get("progreso") or 0)
    examenes  = int(alumno.get("examenes") or 0)
    supera_75 = bool(alumno.get("supera_75"))
    fecha_fin = alumno.get("fecha_fin")
    fecha_ini = alumno.get("fecha_inicio")

    dias_restantes = None
    if fecha_fin:
        try:
            dias_restantes = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
        except Exception:
            pass

    tendencia = "sin datos"
    if len(historial) >= 2:
        delta = historial[-1]["progreso"] - historial[-2]["progreso"]
        tendencia = "subiendo" if delta > 0 else ("bajando" if delta < 0 else "estancado")

    # ── Prompt de análisis ────────────────────────────────────────────────────
    prompt = f"""Eres un experto en seguimiento de formación online. Analiza este alumno y responde en español.

DATOS DEL ALUMNO:
- Nombre: {nombre}
- Curso: {curso}
- Progreso actual: {progreso:.1f}%
- Exámenes realizados: {examenes}
- Supera el 75% requerido: {"Sí" if supera_75 else "No"}
- Fecha inicio: {fecha_ini or "No registrada"}
- Fecha fin: {fecha_fin or "No registrada"}
- Días restantes: {dias_restantes if dias_restantes is not None else "No disponible"}
- Tendencia de progreso: {tendencia}
- Historial de importaciones: {len(historial)} registros

HISTORIAL RECIENTE (últimas 3 entradas):
{json.dumps(historial[-3:], ensure_ascii=False, indent=2) if historial else "Sin historial"}

Responde ÚNICAMENTE con este JSON (sin texto adicional, sin markdown):
{{
  "diagnostico": "párrafo de 2-3 frases con el análisis del alumno",
  "riesgo": "alto|medio|bajo",
  "recomendaciones": ["acción concreta 1", "acción concreta 2", "acción concreta 3"],
  "mensaje_wa": "mensaje WhatsApp completo y personalizado para enviar al alumno, con emojis, máximo 3 párrafos cortos"
}}"""

    texto = _llamar_groq(prompt, max_tokens=600)

    # Parsear JSON de la respuesta
    try:
        # Limpiar posibles backticks de markdown
        limpio = texto.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data   = json.loads(limpio)
    except Exception:
        # Fallback si Gemini no devuelve JSON limpio
        data = {
            "diagnostico":     texto,
            "riesgo":          "medio",
            "recomendaciones": ["Revisar progreso manualmente", "Contactar al alumno"],
            "mensaje_wa":      f"Hola {nombre} 👋, te contactamos para hacer seguimiento de tu curso *{curso}*. Tu progreso actual es {progreso:.0f}%. ¡Estamos aquí para ayudarte!",
        }

    # Añadir emoji de riesgo
    data["riesgo_emoji"] = {"alto": "🔴", "medio": "🟡", "bajo": "🟢"}.get(data.get("riesgo", "medio"), "🟡")
    data["alumno"]       = {"nombre": nombre, "curso": curso, "progreso": progreso,
                             "examenes": examenes, "supera_75": supera_75,
                             "dias_restantes": dias_restantes}
    return data


# ─────────────────────────────────────────────────────────────────────────────
# 2. GENERAR MENSAJE WHATSAPP IA
# ─────────────────────────────────────────────────────────────────────────────

def generar_mensaje_wa(alumno_id: int, contexto: str = "") -> str:
    """
    Genera un mensaje WhatsApp personalizado con IA para un alumno.
    `contexto` puede ser: "motivacion", "urgente", "felicitacion", "recordatorio"
    """
    alumno = _get_alumno(alumno_id)
    if not alumno:
        return "Alumno no encontrado."

    nombre    = alumno.get("nombre", "")
    curso     = alumno.get("curso") or "Sin curso"
    progreso  = float(alumno.get("progreso") or 0)
    fecha_fin = alumno.get("fecha_fin") or "No registrada"
    supera_75 = bool(alumno.get("supera_75"))

    hoy = date.today()
    dias_str = ""
    if alumno.get("fecha_fin"):
        try:
            dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            dias_str = f"{dias} días restantes" if dias >= 0 else f"vencido hace {abs(dias)} días"
        except Exception:
            pass

    tono_map = {
        "motivacion":   "motivador y empático, el alumno tiene bajo progreso y necesita aliento",
        "urgente":      "urgente pero respetuoso, el plazo está muy próximo",
        "felicitacion": "celebratorio y positivo, el alumno superó el 75%",
        "recordatorio": "amable recordatorio de fecha próxima",
    }
    tono = tono_map.get(contexto, "cercano y profesional")

    prompt = f"""Eres el tutor de formación online de {nombre}.
Escribe un mensaje de WhatsApp en español con tono {tono}.

Datos del alumno:
- Curso: {curso}
- Progreso: {progreso:.0f}%
- Fecha fin: {fecha_fin} ({dias_str})
- Supera el 75%: {"Sí" if supera_75 else "No"}

El mensaje debe:
- Empezar con "Hola {nombre} 👋"
- Ser máximo 3 párrafos cortos
- Usar emojis con moderación
- Terminar con una llamada a la acción concreta
- NO usar asteriscos para negritas, escribir en texto plano

Responde solo con el mensaje, sin explicaciones adicionales."""

    return _llamar_groq(prompt, max_tokens=300)


# ─────────────────────────────────────────────────────────────────────────────
# 3. CHATBOT DEL TUTOR
# ─────────────────────────────────────────────────────────────────────────────

# Historial de conversación en memoria por tutor (se reinicia al cerrar sesión)
_chat_sessions: dict[int, list[dict]] = {}


def chatbot_tutor(tutor_id: int, mensaje_usuario: str) -> str:
    """
    Chatbot con contexto de la BD del tutor.
    Mantiene historial de conversación por sesión.
    """
    # Obtener datos frescos del tutor
    resumen = _resumen_tutor(tutor_id)
    alumnos = _get_alumnos_tutor(tutor_id)

    # Agrupar alumnos por curso para que el chatbot pueda filtrar por curso
    from collections import defaultdict
    hoy = date.today()
    cursos_detalle = defaultdict(list)
    for a in alumnos:
        progreso  = float(a.get("progreso") or 0)
        supera    = bool(a.get("supera_75"))
        telefono  = a.get("telefono") or ""
        fecha_fin = a.get("fecha_fin")
        dias      = None
        if fecha_fin:
            try:
                dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            except Exception:
                pass
        cursos_detalle[a.get("curso") or "Sin curso"].append({
            "nombre":   a["nombre"],
            "progreso": f"{progreso:.0f}%",
            "estado":   "✅ Aprobado" if supera else "⚠ Pendiente",
            "dias":     f"{dias}d" if dias is not None else "sin fecha",
            "tel":      telefono or "sin teléfono",
        })

    # Construir sección por curso
    seccion_cursos = ""
    for curso, lista in sorted(cursos_detalle.items()):
        aprobados = sum(1 for a in lista if "✅" in a["estado"])
        seccion_cursos += f"\n### CURSO: {curso} ({len(lista)} alumnos, {aprobados} aprobados)\n"
        for a in lista:
            seccion_cursos += (
                f"  - {a['nombre']} | {a['progreso']} | {a['estado']} | "
                f"Vence: {a['dias']} | Tel: {a['tel']}\n"
            )

    sistema = f"""Eres el asistente IA de un tutor de formación online.
Tienes acceso en tiempo real a TODOS sus alumnos organizados por curso.
Responde en español, de forma concisa y útil.
Hoy es {date.today().strftime('%d/%m/%Y')}.

RESUMEN GLOBAL:
- Total alumnos activos: {resumen.get('total_alumnos', 0)}
- Superan el 75%: {resumen.get('superan_75', 0)} ({resumen.get('pct_exito', 0)}%)
- Sin teléfono: {resumen.get('sin_telefono', 0)}
- Vencidos sin aprobar: {len(resumen.get('vencidos', []))}
- En riesgo crítico (≤14 días y <50%): {len(resumen.get('en_riesgo', []))}

CURSOS Y ALUMNOS (datos reales):
{seccion_cursos}

REGLAS:
- Si preguntan por un curso concreto, filtra y responde solo con alumnos de ese curso.
- Si preguntan por un alumno concreto, busca en todos los cursos.
- Si piden un mensaje WhatsApp, redáctalo completo listo para copiar.
- Si preguntan cuántos cursos hay, lista todos con su tasa de éxito.
- Nunca inventes datos — usa solo los datos proporcionados arriba.
Responde siempre en español."""

    # Historial de conversación
    historial = _chat_sessions.setdefault(tutor_id, [])

    # Construir el prompt con historial (últimos 6 turnos para no sobrepasar límites)
    historial_texto = ""
    for turno in historial[-6:]:
        historial_texto += f"\nTutor: {turno['user']}\nAsistente: {turno['assistant']}\n"

    prompt_completo = f"{sistema}\n\n{historial_texto}\nTutor: {mensaje_usuario}\nAsistente:"

    respuesta = _llamar_groq(prompt_completo, max_tokens=600)

    # Guardar en historial
    historial.append({"user": mensaje_usuario, "assistant": respuesta})

    return respuesta


def limpiar_chat(tutor_id: int) -> None:
    """Reinicia el historial del chat del tutor."""
    _chat_sessions.pop(tutor_id, None)


# ─────────────────────────────────────────────────────────────────────────────
# 4. PREDICCIÓN DE RIESGO MASIVA (para el dashboard)
# ─────────────────────────────────────────────────────────────────────────────

def predecir_riesgo_curso(tutor_id: int, curso: str) -> dict:
    """
    Analiza todos los alumnos de un curso y devuelve:
      - resumen del curso
      - lista de alumnos ordenada por riesgo
      - recomendaciones para el tutor
    """
    conn = sqlite3.connect(FORM_DB)
    conn.row_factory = sqlite3.Row
    alumnos = [dict(a) for a in conn.execute(
        "SELECT * FROM alumnos WHERE tutor_id=? AND curso=? AND (archivado IS NULL OR archivado=0)",
        (tutor_id, curso)
    ).fetchall()]
    conn.close()

    if not alumnos:
        return {"error": f"No hay alumnos activos en el curso '{curso}'"}

    hoy = date.today()
    datos_alumnos = []
    for a in alumnos:
        progreso  = float(a.get("progreso") or 0)
        fecha_fin = a.get("fecha_fin")
        dias      = None
        if fecha_fin:
            try:
                dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            except Exception:
                pass
        datos_alumnos.append({
            "nombre":    a.get("nombre", ""),
            "progreso":  progreso,
            "examenes":  int(a.get("examenes") or 0),
            "supera_75": bool(a.get("supera_75")),
            "dias_restantes": dias,
            "tiene_telefono": bool(a.get("telefono")),
        })

    prompt = f"""Eres un experto en análisis de formación online. Analiza este curso y responde en español.

CURSO: {curso}
TOTAL ALUMNOS: {len(datos_alumnos)}
FECHA HOY: {hoy.strftime('%d/%m/%Y')}

ALUMNOS:
{json.dumps(datos_alumnos, ensure_ascii=False, indent=2)}

Responde ÚNICAMENTE con este JSON (sin texto adicional, sin markdown):
{{
  "resumen": "análisis del curso en 2-3 frases",
  "nivel_riesgo_curso": "alto|medio|bajo",
  "alumnos_criticos": ["nombre1", "nombre2"],
  "recomendaciones": ["acción 1 para el tutor", "acción 2", "acción 3"],
  "accion_prioritaria": "la única acción más urgente que debe hacer el tutor hoy"
}}"""

    texto = _llamar_groq(prompt, max_tokens=500)

    try:
        limpio = texto.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data   = json.loads(limpio)
    except Exception:
        data = {
            "resumen":             texto,
            "nivel_riesgo_curso":  "medio",
            "alumnos_criticos":    [],
            "recomendaciones":     ["Revisar alumnos con progreso bajo"],
            "accion_prioritaria":  "Contactar alumnos con menos progreso",
        }

    data["curso"]   = curso
    data["total"]   = len(alumnos)
    data["emoji"]   = {"alto": "🔴", "medio": "🟡", "bajo": "🟢"}.get(
                        data.get("nivel_riesgo_curso", "medio"), "🟡")
    return data


# ─────────────────────────────────────────────────────────────────────────────
# 5. RESUMEN SEMANAL DEL GRUPO
# ─────────────────────────────────────────────────────────────────────────────

def resumen_semanal(tutor_id: int) -> str:
    """Genera un resumen semanal narrativo de todos los alumnos del tutor."""
    resumen = _resumen_tutor(tutor_id)
    if not resumen:
        return "No hay alumnos activos para generar el resumen."

    prompt = f"""Eres el asistente de un tutor de formación online. 
Genera un resumen semanal ejecutivo en español, claro y accionable.

DATOS DE ESTA SEMANA:
- Total alumnos: {resumen['total_alumnos']}
- Superan 75%: {resumen['superan_75']} ({resumen['pct_exito']}%)
- Sin teléfono: {resumen['sin_telefono']}
- Cursos vencidos sin aprobar: {len(resumen['vencidos'])}
- Alumnos en riesgo crítico: {len(resumen['en_riesgo'])}
- Por vencer en 30 días: {len(resumen['por_vencer'])}

CURSOS:
{json.dumps(resumen['cursos'], ensure_ascii=False, indent=2)}

ALUMNOS EN RIESGO CRÍTICO:
{json.dumps(resumen['en_riesgo'][:10], ensure_ascii=False)}

El resumen debe tener:
1. Un titular con emoji que resuma el estado general (🟢/🟡/🔴)
2. Párrafo de situación general (2-3 frases)
3. Puntos clave: logros de la semana y problemas detectados
4. Las 3 acciones prioritarias para esta semana
Máximo 250 palabras. Tono profesional pero cercano."""

    return _llamar_groq(prompt, max_tokens=500)


# ─────────────────────────────────────────────────────────────────────────────
# 6. RANKING DE ALUMNOS POR RIESGO
# ─────────────────────────────────────────────────────────────────────────────

def ranking_riesgo(tutor_id: int) -> list[dict]:
    """
    Devuelve todos los alumnos ordenados por nivel de riesgo calculado.
    El riesgo se calcula con lógica determinista (sin llamada a IA)
    para que sea instantáneo.
    """
    alumnos  = _get_alumnos_tutor(tutor_id)
    hoy      = date.today()
    ranking  = []

    for a in alumnos:
        progreso  = float(a.get("progreso") or 0)
        supera    = bool(a.get("supera_75"))
        fecha_fin = a.get("fecha_fin")
        nombre    = a.get("nombre", "")
        curso     = a.get("curso") or "Sin curso"
        telefono  = a.get("telefono") or ""
        dias      = None

        if fecha_fin:
            try:
                dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            except Exception:
                pass

        # Calcular score de riesgo (mayor = más riesgo)
        score = 0
        if supera:
            score = 0
        else:
            if dias is not None and dias < 0:       score += 100
            elif dias is not None and dias <= 7:    score += 80
            elif dias is not None and dias <= 14:   score += 60
            elif dias is not None and dias <= 30:   score += 40
            if progreso < 25:   score += 40
            elif progreso < 50: score += 20
            elif progreso < 75: score += 10
            if not telefono:    score += 15

        if supera:
            nivel, emoji = "bajo",  "🟢"
        elif score >= 100:
            nivel, emoji = "alto",  "🔴"
        elif score >= 50:
            nivel, emoji = "medio", "🟡"
        else:
            nivel, emoji = "bajo",  "🟢"

        ranking.append({
            "id":             a.get("id"),
            "nombre":         nombre,
            "curso":          curso,
            "progreso":       progreso,
            "examenes":       int(a.get("examenes") or 0),
            "supera_75":      supera,
            "dias_restantes": dias,
            "telefono":       telefono,
            "score":          score,
            "nivel_riesgo":   nivel,
            "emoji":          emoji,
        })

    ranking.sort(key=lambda x: x["score"], reverse=True)
    return ranking


# ─────────────────────────────────────────────────────────────────────────────
# 7. MENSAJES WHATSAPP MASIVOS
# ─────────────────────────────────────────────────────────────────────────────

def mensajes_wa_masivos(tutor_id: int, filtro: str = "en_riesgo") -> list[dict]:
    """
    Genera mensajes WA personalizados para un grupo de alumnos.
    filtro: "en_riesgo" | "sin_aprobar" | "todos"
    Devuelve lista de {nombre, curso, telefono, mensaje}
    """
    alumnos = _get_alumnos_tutor(tutor_id)
    hoy     = date.today()
    grupo   = []

    for a in alumnos:
        progreso  = float(a.get("progreso") or 0)
        supera    = bool(a.get("supera_75"))
        telefono  = a.get("telefono") or ""
        fecha_fin = a.get("fecha_fin")
        dias      = None

        if fecha_fin:
            try:
                dias = (date.fromisoformat(str(fecha_fin)[:10]) - hoy).days
            except Exception:
                pass

        incluir = False
        if filtro == "todos":
            incluir = bool(telefono)
        elif filtro == "sin_aprobar":
            incluir = not supera and bool(telefono)
        elif filtro == "en_riesgo":
            en_riesgo = (
                not supera and telefono and (
                    (dias is not None and dias <= 30) or progreso < 50
                )
            )
            incluir = bool(en_riesgo)

        if incluir:
            grupo.append(a)

    if not grupo:
        return []

    # Construir prompt con todos los alumnos del grupo
    datos = [{
        "nombre":   a.get("nombre",""),
        "curso":    a.get("curso") or "Sin curso",
        "progreso": float(a.get("progreso") or 0),
        "dias_restantes": None,
    } for a in grupo]

    # Calcular días para cada uno
    for i, a in enumerate(grupo):
        if a.get("fecha_fin"):
            try:
                datos[i]["dias_restantes"] = (
                    date.fromisoformat(str(a["fecha_fin"])[:10]) - hoy
                ).days
            except Exception:
                pass

    prompt = f"""Eres un tutor de formación online. 
Genera mensajes de WhatsApp personalizados en español para cada alumno de esta lista.
Cada mensaje debe:
- Empezar con "Hola [nombre] 👋"
- Mencionar su curso y progreso específico
- Tener máximo 3 párrafos cortos
- Terminar con una llamada a la acción concreta
- Usar texto plano (sin asteriscos para negritas)
- Tono cercano y motivador

ALUMNOS:
{json.dumps(datos, ensure_ascii=False, indent=2)}

Responde ÚNICAMENTE con este JSON (sin texto adicional, sin markdown):
[
  {{"nombre": "nombre del alumno", "mensaje": "mensaje completo"}},
  ...
]"""

    texto = _llamar_groq(prompt, max_tokens=3000)

    try:
        limpio   = texto.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        mensajes = json.loads(limpio)
    except Exception:
        mensajes = [{"nombre": a.get("nombre",""), "mensaje": ""} for a in grupo]

    # Cruzar con teléfonos
    tel_map = {a.get("nombre",""): a.get("telefono","") for a in grupo}
    cur_map = {a.get("nombre",""): a.get("curso","")    for a in grupo}

    resultado = []
    for m in mensajes:
        nombre = m.get("nombre","")
        tel    = tel_map.get(nombre, "")
        resultado.append({
            "nombre":   nombre,
            "curso":    cur_map.get(nombre,""),
            "telefono": tel,
            "mensaje":  m.get("mensaje",""),
            "wa_link":  ("https://wa.me/" + tel.replace(" ","").replace("+","").replace("-","") + "?text=" + urllib.parse.quote(m.get("mensaje",""))) if tel else None,
        })

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# 8. COMPARATIVA ENTRE CURSOS
# ─────────────────────────────────────────────────────────────────────────────

def comparativa_cursos(tutor_id: int) -> dict:
    """Analiza y compara todos los cursos activos del tutor."""
    resumen = _resumen_tutor(tutor_id)
    cursos  = resumen.get("cursos", {})

    if len(cursos) < 2:
        return {"error": "Necesitas al menos 2 cursos activos para comparar."}

    prompt = f"""Eres un experto en análisis de formación online.
Compara estos cursos y da recomendaciones al tutor. Responde en español.

CURSOS ACTIVOS:
{json.dumps(cursos, ensure_ascii=False, indent=2)}

Responde ÚNICAMENTE con este JSON (sin texto adicional, sin markdown):
{{
  "mejor_curso":    "nombre del curso con mejor rendimiento",
  "peor_curso":     "nombre del curso que más atención necesita",
  "analisis":       "párrafo comparativo de 2-3 frases",
  "insight":        "observación clave que el tutor quizás no ha notado",
  "recomendaciones": ["acción específica para el curso con peor rendimiento", "acción para mantener el mejor", "acción general"]
}}"""

    texto = _llamar_groq(prompt, max_tokens=400)

    try:
        limpio = texto.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data   = json.loads(limpio)
    except Exception:
        data = {"analisis": texto, "recomendaciones": []}

    data["cursos"] = cursos
    return data


# ─────────────────────────────────────────────────────────────────────────────
# 9. SUGERENCIAS DE ACCIÓN PARA HOY
# ─────────────────────────────────────────────────────────────────────────────

def sugerencias_hoy(tutor_id: int) -> list[dict]:
    """
    Genera una lista priorizada de acciones concretas para hacer hoy.
    Combina lógica determinista + IA para las descripciones.
    """
    resumen = _resumen_tutor(tutor_id)
    hoy     = date.today().strftime("%d/%m/%Y")

    prompt = f"""Eres el asistente personal de un tutor de formación online.
Hoy es {hoy}. Genera una lista de máximo 6 acciones concretas y priorizadas para hacer HOY.

SITUACIÓN ACTUAL:
- Alumnos vencidos sin aprobar: {len(resumen.get('vencidos',[]))} → {json.dumps([v['nombre'] for v in resumen.get('vencidos',[])[:5]], ensure_ascii=False)}
- Alumnos en riesgo crítico (≤14 días, <50%): {len(resumen.get('en_riesgo',[]))} → {json.dumps([v['nombre'] for v in resumen.get('en_riesgo',[])[:5]], ensure_ascii=False)}
- Por vencer en 30 días: {len(resumen.get('por_vencer',[]))}
- Sin teléfono: {resumen.get('sin_telefono',0)}
- Tasa de éxito global: {resumen.get('pct_exito',0)}%

Responde ÚNICAMENTE con este JSON (sin texto adicional, sin markdown):
[
  {{
    "prioridad": 1,
    "emoji": "🔴|🟡|🟢|📞|📊|💬",
    "titulo": "acción corta (máx 8 palabras)",
    "descripcion": "qué hacer exactamente y por qué (1-2 frases)",
    "tipo": "contacto|revision|admin|seguimiento"
  }}
]"""

    texto = _llamar_groq(prompt, max_tokens=600)

    try:
        limpio = texto.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data   = json.loads(limpio)
    except Exception:
        data = [{"prioridad": 1, "emoji": "📊", "titulo": "Revisar alumnos en riesgo",
                 "descripcion": texto, "tipo": "revision"}]

    return data


# ─────────────────────────────────────────────────────────────────────────────
# 10. CRUCE DE TELÉFONOS DESDE EXCEL
# ─────────────────────────────────────────────────────────────────────────────

def importar_telefonos_excel(archivo_bytes: bytes, tutor_id: int, db_path: str | None = None) -> dict:
    """
    Lee un Excel mínimo con columnas Nombre | Teléfono y actualiza
    los teléfonos en la BD cruzando por nombre normalizado.
    Devuelve estadísticas del cruce.
    """
    import openpyxl
    import io
    import unicodedata

    _db = db_path or FORM_DB

    def _norm(s):
        if not s: return ""
        return " ".join(
            "".join(c for c in unicodedata.normalize("NFD", str(s))
                    if unicodedata.category(c) != "Mn")
            .lower().split()
        )

    # Intentar abrir como xlsx; si falla, intentar como xls antiguo
    try:
        wb   = openpyxl.load_workbook(io.BytesIO(archivo_bytes), data_only=True)
        ws   = wb.active
        rows = list(ws.iter_rows(values_only=True))
    except Exception:
        # Fallback: leer como .xls con xlrd
        try:
            import xlrd
            book  = xlrd.open_workbook(file_contents=archivo_bytes)
            sheet = book.sheet_by_index(0)
            rows  = [tuple(sheet.row_values(i)) for i in range(sheet.nrows)]
        except Exception as e2:
            return {"error": f"No se pudo leer el archivo Excel: {str(e2)}. Asegúrate de guardar como .xlsx"}

    if not rows:
        return {"error": "El archivo está vacío."}

    # Detectar columnas nombre y teléfono
    headers      = [str(c).lower().strip() if c else "" for c in rows[0]]
    idx_nombre   = next((i for i, h in enumerate(headers) if any(p in h for p in ["nombre","alumno","estudiante"])), None)
    idx_telefono = next((i for i, h in enumerate(headers) if any(p in h for p in ["telefono","teléfono","celular","phone","whatsapp","movil","tel"])), None)

    if idx_nombre is None:
        return {"error": "No se encontró columna 'Nombre' en el Excel."}
    if idx_telefono is None:
        return {"error": "No se encontró columna 'Teléfono' en el Excel."}

    # Construir mapa nombre_norm → teléfono
    tel_map = {}
    for fila in rows[1:]:
        if not any(fila): continue
        nombre = fila[idx_nombre]
        tel    = fila[idx_telefono]
        if nombre and tel:
            t = str(tel).strip().replace(".0","").replace(" ","")
            if t and t.lower() not in ("none","nan",""):
                tel_map[_norm(nombre)] = t

    if not tel_map:
        return {"error": "No se encontraron teléfonos válidos en el archivo."}

    # Actualizar BD
    conn     = sqlite3.connect(_db)
    conn.row_factory = sqlite3.Row
    alumnos  = conn.execute(
        "SELECT id, nombre FROM alumnos WHERE tutor_id=? AND (archivado IS NULL OR archivado=0)",
        (tutor_id,)
    ).fetchall()

    actualizados = 0
    no_encontrados = []

    for alumno in alumnos:
        clave = _norm(alumno["nombre"])
        if clave in tel_map:
            conn.execute("UPDATE alumnos SET telefono=? WHERE id=?",
                         (tel_map[clave], alumno["id"]))
            actualizados += 1
        else:
            no_encontrados.append(alumno["nombre"])

    # Teléfonos del Excel que no cruzaron con ningún alumno
    nombres_bd   = {_norm(a["nombre"]) for a in alumnos}
    sin_cruzar   = [nombre for nombre in tel_map if nombre not in nombres_bd]

    conn.commit()
    conn.close()

    return {
        "total_excel":    len(tel_map),
        "actualizados":   actualizados,
        "no_encontrados": no_encontrados[:20],
        "sin_cruzar":     sin_cruzar[:20],
    }