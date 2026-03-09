"""
Analyzer — Claude API powered opportunity detection engine.
Analyzes chat history to find revenue opportunities and build user profile.
"""
import os
import json
import logging
import anthropic
from datetime import datetime

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-20250514"

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Robert's business context for the analyzer
BUSINESS_CONTEXT = """
КОНТЕКСТ БИЗНЕСА (для анализа переписок):

Роберт — основатель ZBS Media / Premium Stuff Production в Ташкенте.
~16 человек в команде. Цель: выйти из операционки, масштабировать бизнес, 
личный доход $3-5K/мес.

ПРОЕКТЫ И ПОТЕНЦИАЛЬНЫЙ ДОХОД:
1. ZBS Media — новостной/развлекательный контент
   - @zbspodcast (83K подписчиков Instagram) — основная площадка для рекламных размещений
   - @zbsnewz.uz (28K), @zbsnewz (25K) — новостные
   - Telegram каналы — доп. размещения
   - Модель дохода: рекламные размещения ($200-1000 за пост/сторис), спецпроекты

2. Plan Banan — детская анимация @planbananuz (6.7K)
   - 25+ эпизодов, культуры Узбекистана
   - Модель дохода: спонсорство (ищут Musaffo), бренд-интеграции, гранты

3. #SaveCharvak — экопроект, уборка водохранилища Чарвак
   - UzAuto Motors спонсор (~$20K/выезд)
   - 14 зон, 7 месяцев расписание
   - Модель дохода: корп. спонсорство, медиа-партнёрства

4. Коммерческое производство — видеореклама, тендеры, бренд-фильмы
   - Pepsi — единственный активный рекурринг
   - Камеры: RED Komodo 6K, Sony A7S III
   - Модель дохода: проектная оплата ($2K-20K за проект)

5. TrabajaYa — рекрутинговая платформа с чат-ботом
   - На стадии развития

КЛЮЧЕВЫЕ ЛЮДИ:
- Даня — единственный видеомонтажёр (риск!)
- Лазиза — ассистент/sales менеджер
- Арслан — бэкап-редактор, своя продакшн-компания, берёт крупные коммерческие
- Самвел — энергичный, переводится в продажи рекламных размещений
- Настя — AI-автоматизация, еженедельные сессии
- Сусанна — бухгалтерия (неполная — не трекает коммерческие проекты)
- Вадим — сценарии Plan Banan
- Ирода — анимация Plan Banan
- Сарик Юсупов — контент-мейкер

ФИНАНСОВЫЕ ЦЕЛИ 2026:
- Поездка в Китай апр-май $2K
- MacBook Pro M6 $3.5K  
- iPhone $2K
- Пианино $1K
- Диван $1K
Итого нужно ~$15K сверх расходов

РАСХОДЫ: ~$1,500/мес

ПСИХОЛОГИЯ РОБЕРТА (для калибровки рекомендаций):
- Мотивация резко растёт когда видит прямую связь с деньгами
- Предпочитает автоматизированные системы, а не ручные процессы
- Прямой, результат-ориентированный
- Нетерпелив к лишним шагам
- Принимает решения быстро когда видит выгоду
- Лучше работает с конкретными суммами и дедлайнами
"""

OPPORTUNITY_EXTRACTION_PROMPT = """Ты — Revenue Opportunity Analyst для медиа-продакшн компании в Ташкенте.

{context}

ЗАДАЧА: Проанализируй переписку из чата "{chat_name}" и найди УПУЩЕННЫЕ ВОЗМОЖНОСТИ ДЛЯ ЗАРАБОТКА.

Ищи:
1. **Незавершённые сделки** — кто-то спрашивал про рекламу/съёмку/сотрудничество, но разговор заглох
2. **Потенциальные клиенты** — кто проявил интерес но не получил предложение
3. **Партнёрства** — упоминания совместных проектов, которые не реализовались
4. **Апсейл существующих** — текущие клиенты которым можно продать больше
5. **Спонсорские возможности** — бренды и компании упомянутые в контексте проектов
6. **Контент-монетизация** — идеи контента которые можно монетизировать
7. **Тендеры/гос.заказы** — любые упоминания тендеров или гос.проектов
8. **Делегируемые задачи** — вещи которые Роберт делает сам, но может делегировать для масштабирования

КРИТИЧЕСКИ ВАЖНО:
- Каждая возможность ДОЛЖНА иметь конкретную привязку к деньгам (даже приблизительную)
- Каждая возможность ДОЛЖНА иметь конкретный первый шаг (написать кому, что, зачем)
- Учитывай психологию Роберта: он мотивирован деньгами и конкретикой
- Не выдумывай возможности — только то что реально следует из переписки
- Приоритизируй quick wins (быстрый результат) выше долгих проектов

ПЕРЕПИСКА:
{messages}

Ответь СТРОГО в JSON формате (без markdown, без ```):
{{
  "opportunities": [
    {{
      "project": "zbs_media|plan_banan|savecharvak|commercial|trabaja|general",
      "title": "Краткое название возможности",
      "description": "Что конкретно за возможность, почему она ценна",
      "action_items": [
        "Конкретный шаг 1: написать [кому] в [где] насчёт [что]",
        "Конкретный шаг 2: ..."
      ],
      "contact_person": "Имя человека с кем нужно связаться",
      "contact_handle": "@username или номер если есть",
      "potential_revenue": "$X-Y (описание)",
      "revenue_low": 100,
      "revenue_high": 500,
      "confidence": "high|medium|low",
      "source_snippet": "Ключевая цитата из переписки (1-2 предложения)",
      "reasoning": "Почему это хорошая возможность именно для Роберта",
      "priority": 1-10,
      "tags": ["quick_win", "recurring", "one_time", "partnership", "upsell", "new_client"]
    }}
  ],
  "profile_insights": {{
    "communication_style": "Наблюдения о стиле общения Роберта в этом чате",
    "energy_patterns": "Когда и на что реагирует с энтузиазмом",
    "blind_spots": "Что систематически упускает или откладывает",
    "strengths": "Что делает хорошо в переговорах/коммуникации"
  }}
}}
"""

DAILY_PLAN_PROMPT = """Ты — персональный Revenue Coach Роберта.

{context}

ПРОФИЛЬ РОБЕРТА (из анализа переписок):
{profile}

АКТИВНЫЕ ВОЗМОЖНОСТИ В PIPELINE:
{opportunities}

СТАТИСТИКА:
{stats}

Сегодня: {today}

ЗАДАЧА: Составь план действий на сегодня. 

ПРАВИЛА:
1. Максимум 5 задач — не перегружай
2. Каждая задача привязана к конкретной сумме
3. Начни с самой быстрой/лёгкой (quick win для разгона)
4. Для каждой задачи: ЧТО сделать, КОМУ написать, КАКОЙ месседж отправить
5. Формат общения — как дружеский пинок: прямо, с юмором, с мотивацией
6. В конце — итого потенциальный доход если всё сделать
7. Привязывай к целям 2026 (Китай $2K, MacBook $3.5K и т.д.)

Формат ответа — обычный текст (будет отправлен в Telegram), используй эмодзи умеренно.
Структура:
🎯 ПЛАН НА [дата]

1️⃣ [#ID] [Задача] — $XXX потенциал
   → [Конкретное действие]
   → [Готовый текст сообщения если нужно]

...

ВАЖНО: Перед каждой задачей ОБЯЗАТЕЛЬНО укажи #ID возможности из pipeline (например #8, #311). Это критично для трекинга.

💰 Итого потенциал дня: $XXX
📊 Прогресс к целям: [что приближает]
"""


async def analyze_chat(chat_name: str, messages_text: str, existing_profile: dict = None) -> dict:
    """
    Analyze a single chat's messages and extract opportunities.
    Returns parsed JSON with opportunities and profile insights.
    """
    prompt = OPPORTUNITY_EXTRACTION_PROMPT.format(
        context=BUSINESS_CONTEXT,
        chat_name=chat_name,
        messages=messages_text
    )
    
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )
        
        text = response.content[0].text.strip()
        
        # Clean up potential markdown wrapping
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        
        result = json.loads(text)
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response for {chat_name}: {e}")
        logger.error(f"Raw response: {text[:500]}")
        return {"opportunities": [], "profile_insights": {}}
    except Exception as e:
        logger.error(f"Claude API error for {chat_name}: {e}")
        return {"opportunities": [], "profile_insights": {}}


async def generate_daily_plan(opportunities: list, profile: dict, stats: dict, feedback: list = None) -> str:
    """
    Generate today's action plan based on active opportunities.
    Returns formatted text for Telegram.
    """
    # Format opportunities for the prompt
    opp_text = ""
    for i, opp in enumerate(opportunities, 1):
        actions = json.loads(opp["action_items"]) if isinstance(opp["action_items"], str) else opp["action_items"]
        actions_str = "\n      ".join(actions) if actions else "Нет конкретных шагов"
        opp_text += f"""
    {i}. [{opp['project']}] {opp['title']}
       Потенциал: {opp['potential_revenue']} (${opp['revenue_low']}-${opp['revenue_high']})
       Уверенность: {opp['confidence']}
       Контакт: {opp.get('contact_person', 'N/A')} {opp.get('contact_handle', '')}
       Шаги: {actions_str}
       ID: #{opp['id']}
    """
    
    # Format profile
    profile_text = "\n".join(f"- {k}: {v}" for k, v in profile.items()) if profile else "Профиль ещё не сформирован"
    
    # Format stats
    stats_text = f"""
    Новых возможностей: {stats.get('new_count', 0)}
    В работе: {stats.get('in_progress', 0)}
    Выполнено: {stats.get('done_count', 0)}
    Пропущено: {stats.get('skipped_count', 0)}
    Pipeline: ${stats.get('revenue_pipeline_low', 0)}-${stats.get('revenue_pipeline_high', 0)}
    Реализовано: ${stats.get('revenue_realized_low', 0)}-${stats.get('revenue_realized_high', 0)}
    """
    
    today = datetime.now().strftime("%d %B %Y, %A")
    
    # Format feedback for self-learning
    feedback_text = ""
    if feedback:
        feedback_text = "\n\nОБРАТНАЯ СВЯЗЬ РОБЕРТА (учти при выборе задач!):\n"
        for fb in feedback:
            feedback_text += f"- Отклонил '{fb.get('title', '?')}' ({fb.get('project', '?')}) — причина: {fb.get('reason', '?')}\n"
        feedback_text += "\nНЕ предлагай похожие задачи! Адаптируй рекомендации под обратную связь."
    
    prompt = DAILY_PLAN_PROMPT.format(
        context=BUSINESS_CONTEXT,
        profile=profile_text,
        opportunities=opp_text,
        stats=stats_text,
        today=today
    ) + feedback_text
    
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            temperature=0.5,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
        
    except Exception as e:
        logger.error(f"Failed to generate daily plan: {e}")
        return "❌ Не удалось сгенерировать план. Попробуй /plan позже."


async def analyze_single_opportunity(opp_text: str) -> str:
    """Quick analysis of a single opportunity — deeper dive."""
    prompt = f"""{BUSINESS_CONTEXT}

Роберт спрашивает подробнее про эту возможность:
{opp_text}

Дай развёрнутый анализ:
1. Почему это стоит сделать (с привязкой к целям)
2. Риски и подводные камни
3. Пошаговый план реализации (каждый шаг — конкретное действие)
4. Готовые шаблоны сообщений для отправки
5. Ожидаемый timeline от действия до денег

Отвечай как прямой бизнес-консультант. Коротко и по делу."""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.error(f"Failed to analyze opportunity: {e}")
        return "❌ Ошибка анализа."
