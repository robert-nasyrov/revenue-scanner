"""
Revenue Opportunity Scanner — Telegram Bot v2
- Buttons match plan tasks
- Skip asks for feedback → system learns
- Weekly auto-scan (no manual /scan)
- Self-learning from user feedback
"""
import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from telegram.constants import ParseMode

import database as db
from scanner import scan_all_work_chats, chunk_messages, format_messages_for_analysis
from analyzer import analyze_chat, generate_daily_plan, analyze_single_opportunity

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "271065518"))
WEEKLY_SCAN_DAY = int(os.getenv("WEEKLY_SCAN_DAY", "0"))
WEEKLY_SCAN_HOUR = int(os.getenv("WEEKLY_SCAN_HOUR", "3"))

pool = None


def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != OWNER_ID:
            if update.message:
                await update.message.reply_text("⛔ Доступ только для владельца.")
            return
        return await func(update, context)
    return wrapper


async def post_init(app: Application):
    global pool
    env_check = {
        "DATABASE_URL": bool(os.getenv("DATABASE_URL")),
        "TELEGRAM_BOT_TOKEN": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "TELEGRAM_API_ID": bool(os.getenv("TELEGRAM_API_ID")),
        "TELEGRAM_API_HASH": bool(os.getenv("TELEGRAM_API_HASH")),
        "TELEGRAM_STRING_SESSION": bool(os.getenv("TELEGRAM_STRING_SESSION")),
        "ANTHROPIC_API_KEY": bool(os.getenv("ANTHROPIC_API_KEY")),
    }
    logger.info(f"Environment check: {env_check}")
    try:
        pool = await db.get_pool()
        await db.init_db(pool)
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database init failed: {e}")
        pool = None
    asyncio.create_task(weekly_scan_scheduler(app))


# ═══════════════════════════════════════════
# WEEKLY AUTO-SCAN
# ═══════════════════════════════════════════

async def weekly_scan_scheduler(app: Application):
    while True:
        now = datetime.now(timezone.utc)
        days_ahead = WEEKLY_SCAN_DAY - now.weekday()
        if days_ahead < 0 or (days_ahead == 0 and now.hour >= WEEKLY_SCAN_HOUR):
            days_ahead += 7
        next_scan = now.replace(hour=WEEKLY_SCAN_HOUR, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
        wait_seconds = (next_scan - now).total_seconds()
        logger.info(f"Next weekly scan in {wait_seconds/3600:.1f}h ({next_scan.isoformat()})")
        await asyncio.sleep(wait_seconds)
        logger.info("Starting weekly auto-scan...")
        try:
            await run_scan(app, notify=True)
        except Exception as e:
            logger.error(f"Weekly scan failed: {e}", exc_info=True)


async def run_scan(app: Application, notify=True, scan_days=7):
    if not pool:
        return
    scan_id = await db.save_scan(pool, f"weekly_{scan_days}d")
    import scanner
    old_months = scanner.SCAN_MONTHS
    scanner.SCAN_MONTHS = 1
    try:
        scan_data = await scan_all_work_chats()
        total_chats = len(scan_data["chats"])
        total_messages = scan_data["total_messages"]
        all_opportunities = 0
        for chat_name, chat_data in scan_data["chats"].items():
            messages = chat_data["messages"]
            if scan_days < 180:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=scan_days)).isoformat()
                messages = [m for m in messages if m.get("date", "") >= cutoff]
            if not messages:
                continue
            chunks = chunk_messages(messages, chunk_size=150)
            for chunk in chunks:
                messages_text = format_messages_for_analysis(chunk)
                if len(messages_text) < 100:
                    continue
                result = await analyze_chat(chat_name, messages_text)
                for opp in result.get("opportunities", []):
                    opp["source_chat"] = chat_name
                    if chunk:
                        opp["source_date"] = chunk[0].get("date")
                    is_dup = await db.check_duplicate(pool, opp["title"], chat_name)
                    if not is_dup:
                        await db.save_opportunity(pool, opp)
                        all_opportunities += 1
                insights = result.get("profile_insights", {})
                for key, value in insights.items():
                    if value:
                        await db.save_profile_insight(pool, f"{chat_name}_{key}", value)
                await asyncio.sleep(1)
        await db.complete_scan(pool, scan_id, total_chats, total_messages, all_opportunities)
        if notify and all_opportunities > 0:
            try:
                stats = await db.get_stats(pool)
                await app.bot.send_message(
                    OWNER_ID,
                    f"🔄 <b>Еженедельный скан завершён!</b>\n\n"
                    f"📊 {total_chats} чатов, {total_messages} сообщений\n"
                    f"💡 Новых возможностей: <b>{all_opportunities}</b>\n"
                    f"💰 Pipeline: ${stats['revenue_pipeline_low']:,}-${stats['revenue_pipeline_high']:,}\n\n"
                    f"/plan — план на сегодня",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to notify: {e}")
    finally:
        scanner.SCAN_MONTHS = old_months


# ═══════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════

@owner_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔍 <b>Revenue Opportunity Scanner v2</b>\n\n"
        "Скан автоматический — каждый понедельник.\n\n"
        "/plan — План действий на сегодня\n"
        "/pipeline — Активные возможности\n"
        "/opp [id] — Подробнее\n"
        "/done [id] — Выполнено\n"
        "/skip [id] — Пропустить\n"
        "/stats — Статистика\n"
        "/projects — По проектам\n"
        "/profile — Твой профиль\n"
        "/rescan — Ручной скан (7 дней)",
        parse_mode=ParseMode.HTML
    )


@owner_only
async def cmd_rescan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔄 Сканирую за последнюю неделю...")
    try:
        await run_scan(context.application, notify=False, scan_days=7)
        stats = await db.get_stats(pool)
        await msg.edit_text(
            f"✅ Скан завершён!\n💰 Pipeline: ${stats['revenue_pipeline_low']:,}-${stats['revenue_pipeline_high']:,}\n/plan — план"
        )
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")


@owner_only
async def cmd_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🧠 Генерирую план на сегодня...")
    try:
        feedback = await db.get_recent_feedback(pool, limit=20)
        opportunities = await db.get_active_opportunities(pool, limit=15)
        if not opportunities:
            await msg.edit_text("📭 Pipeline пуст. Подожди автоскан в понедельник.")
            return
        profile = await db.get_profile(pool)
        stats = await db.get_stats(pool)
        plan = await generate_daily_plan(
            [dict(o) for o in opportunities], profile, stats, feedback=feedback
        )
        # Extract #IDs from plan text to match buttons
        plan_ids = re.findall(r'#(\d+)', plan)
        seen = set()
        unique_ids = []
        for pid in plan_ids:
            if pid not in seen:
                seen.add(pid)
                unique_ids.append(int(pid))
        plan_opp_ids = unique_ids[:5]
        # Build buttons matching plan order
        keyboard = []
        for opp_id in plan_opp_ids:
            keyboard.append([
                InlineKeyboardButton(f"✅ Сделал #{opp_id}", callback_data=f"done_{opp_id}"),
                InlineKeyboardButton(f"❌ Не буду #{opp_id}", callback_data=f"skipask_{opp_id}"),
            ])
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        if len(plan) > 4000:
            parts = [plan[i:i+4000] for i in range(0, len(plan), 4000)]
            await msg.edit_text(parts[0])
            for i, part in enumerate(parts[1:]):
                rm = reply_markup if i == len(parts) - 2 else None
                await update.message.reply_text(part, reply_markup=rm)
        else:
            await msg.edit_text(plan, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Plan generation failed: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")


@owner_only
async def cmd_pipeline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    opportunities = await db.get_active_opportunities(pool, limit=20)
    if not opportunities:
        await update.message.reply_text("📭 Pipeline пуст.")
        return
    text = "📋 <b>PIPELINE</b>\n\n"
    total_low, total_high = 0, 0
    for opp in opportunities:
        se = "🆕" if opp["status"] == "new" else "🔄"
        ce = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(opp["confidence"], "⚪")
        text += f"{se} <b>#{opp['id']}</b> {opp['title']}\n   {ce} {opp['project']} | ${opp['revenue_low']}-${opp['revenue_high']}\n\n"
        total_low += opp["revenue_low"]
        total_high += opp["revenue_high"]
    text += f"💰 <b>Итого: ${total_low:,}-${total_high:,}</b>"
    if len(text) > 4000:
        for part in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await update.message.reply_text(part, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@owner_only
async def cmd_opp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи ID: /opp 5")
        return
    opp_id = int(context.args[0].replace("#", ""))
    opp = await db.get_opportunity_by_id(pool, opp_id)
    if not opp:
        await update.message.reply_text(f"#{opp_id} не найдена.")
        return
    actions = json.loads(opp["action_items"]) if isinstance(opp["action_items"], str) else opp["action_items"]
    actions_text = "\n".join(f"  {i}. {a}" for i, a in enumerate(actions, 1))
    text = (
        f"🔍 <b>#{opp['id']}: {opp['title']}</b>\n\n"
        f"🏷 {opp['project']} | 💰 {opp['potential_revenue']}\n"
        f"👤 {opp.get('contact_person', '-')} {opp.get('contact_handle', '')}\n\n"
        f"{opp['description']}\n\n"
        f"✅ <b>Шаги:</b>\n{actions_text}\n\n"
        f"💡 {opp.get('reasoning', '-')}\n"
        f"📎 {opp.get('source_chat', '-')}"
    )
    keyboard = [
        [
            InlineKeyboardButton("✅ Сделал", callback_data=f"done_{opp['id']}"),
            InlineKeyboardButton("❌ Не буду", callback_data=f"skipask_{opp['id']}"),
        ],
        [InlineKeyboardButton("🧠 Глубокий анализ", callback_data=f"analyze_{opp['id']}")],
    ]
    if len(text) > 4000:
        text = text[:3950] + "..."
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))


@owner_only
async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи ID: /done 5")
        return
    opp_id = int(context.args[0].replace("#", ""))
    await db.mark_done(pool, opp_id)
    opp = await db.get_opportunity_by_id(pool, opp_id)
    if opp:
        await update.message.reply_text(f"✅ #{opp_id} выполнена! +${opp['revenue_low']}-${opp['revenue_high']} 💰")


@owner_only
async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Укажи ID: /skip 5 причина")
        return
    opp_id = int(context.args[0].replace("#", ""))
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "не указана"
    await db.mark_skipped(pool, opp_id, reason)
    await db.save_feedback(pool, opp_id, reason)
    await update.message.reply_text(f"⏭ #{opp_id} пропущена: {reason}")


@owner_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await db.get_stats(pool)
    realized_avg = (stats["revenue_realized_low"] + stats["revenue_realized_high"]) / 2
    progress = (realized_avg / 15000 * 100) if 15000 > 0 else 0
    text = (
        f"📊 <b>СТАТИСТИКА</b>\n\n"
        f"🆕 Новых: {stats['new_count']} | 🔄 В работе: {stats['in_progress']}\n"
        f"✅ Выполнено: {stats['done_count']} | ⏭ Пропущено: {stats['skipped_count']}\n\n"
        f"💰 Pipeline: ${stats['revenue_pipeline_low']:,}-${stats['revenue_pipeline_high']:,}\n"
        f"✅ Реализовано: ${stats['revenue_realized_low']:,}-${stats['revenue_realized_high']:,}\n\n"
        f"🎯 Прогресс к $15K: {'█' * int(progress/5)}{'░' * (20 - int(progress/5))} {progress:.1f}%"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@owner_only
async def cmd_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    projects = ["zbs_media", "plan_banan", "savecharvak", "commercial", "trabaja", "general"]
    text = "🏢 <b>ПО ПРОЕКТАМ</b>\n\n"
    for project in projects:
        opps = await db.get_active_opportunities(pool, limit=5, project=project)
        if opps:
            emoji = {"zbs_media": "📺", "plan_banan": "🍌", "savecharvak": "🌿", "commercial": "🎬", "trabaja": "💼", "general": "📌"}.get(project, "📌")
            tl = sum(o["revenue_low"] for o in opps)
            th = sum(o["revenue_high"] for o in opps)
            text += f"{emoji} <b>{project}</b> ({len(opps)}) — ${tl}-${th}\n"
            for opp in opps[:3]:
                text += f"  • #{opp['id']} {opp['title'][:50]}\n"
            text += "\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@owner_only
async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    profile = await db.get_profile(pool)
    if not profile:
        await update.message.reply_text("📭 Профиль не сформирован.")
        return
    text = "🧠 <b>ПРОФИЛЬ</b>\n\n"
    categories = {}
    for key, value in profile.items():
        parts = key.rsplit("_", 1)
        category = parts[1] if len(parts) == 2 else key
        if category not in categories:
            categories[category] = []
        categories[category].append(value)
    names = {"style": "💬 Стиль", "patterns": "⚡ Паттерны", "spots": "🔴 Слепые зоны", "strengths": "💪 Сильные стороны"}
    for cat, items in categories.items():
        text += f"\n{names.get(cat, cat)}:\n"
        for item in items[:3]:
            text += f"• {item[:200]}\n"
    if len(text) > 4000:
        text = text[:3950] + "..."
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ═══════════════════════════════════════════
# CALLBACKS
# ═══════════════════════════════════════════

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != OWNER_ID:
        await query.answer("⛔")
        return
    data = query.data
    await query.answer()

    if data.startswith("done_"):
        opp_id = int(data.replace("done_", ""))
        await db.mark_done(pool, opp_id)
        opp = await db.get_opportunity_by_id(pool, opp_id)
        await query.edit_message_reply_markup(reply_markup=None)
        if opp:
            await query.message.reply_text(f"✅ #{opp_id} выполнена! +${opp['revenue_low']}-${opp['revenue_high']} 💰")

    elif data.startswith("skipask_"):
        opp_id = int(data.replace("skipask_", ""))
        keyboard = [
            [InlineKeyboardButton("Нереалистично", callback_data=f"skipr_{opp_id}_unrealistic")],
            [InlineKeyboardButton("Не моё", callback_data=f"skipr_{opp_id}_notmine")],
            [InlineKeyboardButton("Неактуально", callback_data=f"skipr_{opp_id}_outdated")],
            [InlineKeyboardButton("Слишком мелко", callback_data=f"skipr_{opp_id}_toosmall")],
            [InlineKeyboardButton("Сделаю позже", callback_data=f"skipr_{opp_id}_later")],
        ]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("skipr_"):
        parts = data.split("_", 2)
        opp_id = int(parts[1])
        reason = parts[2]
        reason_text = {"unrealistic": "Нереалистично", "notmine": "Не моё", "outdated": "Неактуально", "toosmall": "Слишком мелко", "later": "Сделаю позже"}.get(reason, reason)
        if reason == "later":
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text(f"⏰ #{opp_id} — напомню позже")
        else:
            await db.mark_skipped(pool, opp_id, reason_text)
            await db.save_feedback(pool, opp_id, reason_text)
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text(f"⏭ #{opp_id} — {reason_text}. Учту в будущем.")

    elif data.startswith("detail_"):
        opp_id = int(data.replace("detail_", ""))
        opp = await db.get_opportunity_by_id(pool, opp_id)
        if opp:
            actions = json.loads(opp["action_items"]) if isinstance(opp["action_items"], str) else opp["action_items"]
            text = (f"🔍 <b>#{opp['id']}: {opp['title']}</b>\n\n{opp['description']}\n\n"
                    f"💰 {opp['potential_revenue']}\n👤 {opp.get('contact_person', '-')}\n\n"
                    f"Шаги:\n" + "\n".join(f"  {i}. {a}" for i, a in enumerate(actions, 1)))
            if len(text) > 4000:
                text = text[:3950] + "..."
            await query.message.reply_text(text, parse_mode=ParseMode.HTML)

    elif data.startswith("analyze_"):
        opp_id = int(data.replace("analyze_", ""))
        opp = await db.get_opportunity_by_id(pool, opp_id)
        if opp:
            await query.message.reply_text("🧠 Анализирую...")
            opp_text = f"{opp['title']}\n{opp['description']}\nПотенциал: {opp['potential_revenue']}"
            analysis = await analyze_single_opportunity(opp_text)
            if len(analysis) > 4000:
                for part in [analysis[i:i+4000] for i in range(0, len(analysis), 4000)]:
                    await query.message.reply_text(part)
            else:
                await query.message.reply_text(analysis)


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("pipeline", cmd_pipeline))
    app.add_handler(CommandHandler("opp", cmd_opp))
    app.add_handler(CommandHandler("done", cmd_done))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("projects", cmd_projects))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CommandHandler("rescan", cmd_rescan))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(callback_handler))
    logger.info("Revenue Opportunity Scanner v2 starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
