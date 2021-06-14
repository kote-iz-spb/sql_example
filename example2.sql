WITH
pupils_with_webinar AS
-- в данном блоке мы условно разделяем пользователей на принадлежность к вебинарной группе, данные о принадлежности содержатся в json
(
    SELECT
        id,
        CASE
            WHEN (COALESCE(u.other_user_data ->> 'referer', u.cpa ->> 'referer') LIKE '/webinars%'
                OR COALESCE(u.other_user_data ->> 'referer', u.cpa ->> 'referer') LIKE '/stay-home%'
                OR COALESCE(u.other_user_data ->> 'referer', u.cpa ->> 'referer') LIKE '/final_essay%') THEN 'v'
            ELSE 'n'
            END AS vebinar,
        u.create_time
    FROM
        users u                             -- Таблица содержащая информацию о пользователях
        LEFT JOIN data.users_trash ut       -- Таблица содержащая тестовых и прочих мусорных пользователей которх необходимо исключить
                  ON ut.user_id = u.id
    WHERE
        u.role = 'pupil'
        AND ut.user_id IS NULL              -- Этим условием исключаем всех пользователей кто относится к категории тестовые и мусорные
        AND create_time + INTERVAL '3 hours' BETWEEN '2020-01-01 00:00:00' AND '2020-12-31 23:59:59'  -- Диапазон дат за 2020 год
),
registrations AS
-- в данном блоке считаем количество регистраций за весь период
(
    SELECT
        'Line1'   AS group_id,
        COUNT(id) AS reg
    FROM
        pupils_with_webinar
    WHERE
        vebinar = 'n'                       -- условие принадлежности к вебинарной группе было прописано сдесь т.к. при дальнейшем использовании отчета в metabase данное поле участвует в фильтрации
),
canceled_lessons AS
-- В данном блоке выделяем события которые были созданы и отменены, учитывая тот факт что отмены не может быть без создания
(
    SELECT
        eel.event_id,
        COUNT(eel.action_type) % 2 AS cl    -- считаем события с четным/нечетным количеством действий (если значение четное значит событие создано и отменено, если нечетное то отмены не было)
    FROM
        events_editing_log eel              -- Таблица содержащая все действия с событиями (events)
        INNER JOIN events e                 -- Таблица Событий
                   ON e.id = eel.event_id
    WHERE
        e.quark_smell = 'intro'
        AND e.type = 'lesson'
        AND eel.action_type IN ('бронирование урока',
                                'бронирование урока из старого интерфейса',
                                'создание урока',
                                'создание урока из выигранного лота',
                                'создание урока из старого интерфейса',
                                'отмена урока')         -- условия создания/отмены прописанное в таблице действий
    GROUP BY 1
),
events_logs AS
-- в данном блоке нумеруем действия для определения порядка создания и отмены событий
(
    SELECT
        eel.event_id,
        eel.action_type,
        eel.datetime,
        ROW_NUMBER() OVER (PARTITION BY eel.event_id ORDER BY eel.datetime) rn  -- нумеруем действия с группировкой по событиям Нечетные номера будут соответствовать созданию события, а четные его отмене
    FROM
        events e
        INNER JOIN events_editing_log eel
                   ON e.id = eel.event_id
        INNER JOIN canceled_lessons cl
                   ON cl.event_id = e.id
    WHERE
        e.quark_smell = 'intro'
        AND e.type = 'lesson'
        AND cl.cl = 0
        AND eel.action_type IN ('бронирование урока',
                                'бронирование урока из старого интерфейса',
                                'создание урока',
                                'создание урока из выигранного лота',
                                'создание урока из старого интерфейса',
                                'отмена урока')
),
create_canceled_intro AS
-- выделяем события которые были созданы и затем в течении часа отменены.
(
    SELECT DISTINCT
        el1.event_id
    FROM
        events_logs el1
        INNER JOIN events_logs el2
                   ON el1.event_id = el2.event_id
        LEFT JOIN  lessons l
                   ON l.event_id = el1.event_id
    WHERE
        el2.rn - el1.rn = 1             --отслеживаем только соседние события отмены и создания
        AND el1.action_type IN ('бронирование урока',
                                'бронирование урока из старого интерфейса',
                                'создание урока',
                                'создание урока из выигранного лота',
                                'создание урока из старого интерфейса')
        AND el2.action_type = 'отмена урока'
        AND l.event_id IS NULL
        AND EXTRACT(EPOCH FROM (el2.datetime - el1.datetime)) / 60 <= 60
),
auctions AS
-- таблица аукционов розыгрыша событий
(
    SELECT
        a.pupil_id   AS pupil_id,
        a.created_by AS manager,
        a.created_at,
        a.id::text   AS auction_id
    FROM
        auctions a
    WHERE
        event_id IS NOT NULL
),
intro_lessons AS
-- выборка событий из которых исключены те, которые были отменены в течение часа. В этот раз берем только создание событий
(
    SELECT
        pww.id      AS pupil_id,
        eel.user_id AS manager,
        eel.datetime,
        eel.event_id::text
    FROM
        events_editing_log eel
        INNER JOIN events e
                   ON eel.event_id = e.id
        INNER JOIN participants p
                   ON e.id = p.event_id
        INNER JOIN pupils_with_webinar pww
                   ON pww.id = p.user_id
        LEFT JOIN  create_canceled_intro cci
                   ON cci.event_id = e.id
    WHERE
        e.type = 'lesson'
        AND e.quark_smell = 'intro'
        AND cci.event_id IS NULL
        AND eel.action_type IN ('бронирование урока',
                                'бронирование урока из старого интерфейса',
                                'создание урока',
                                'создание урока из выигранного лота',
                                'создание урока из старого интерфейса')
),
auctions_and_intro_lessons AS
-- объединяем аукционы и события
(
    SELECT *
    FROM
        intro_lessons
    UNION
    SELECT *
    FROM
        auctions
),
all_intro_lessons AS
-- нумеруем по порядку события чтобы выделять 1ю и повторную записи
(
    SELECT
        aai.pupil_id,
        aai.manager,
        ROW_NUMBER() OVER (PARTITION BY aai.pupil_id ORDER BY aai.datetime) rn,
        pww.create_time
    FROM
        auctions_and_intro_lessons aai
        INNER JOIN pupils_with_webinar pww
                   ON pww.id = aai.pupil_id
    WHERE
        pww.vebinar = 'n'
),
managers AS
(
    SELECT
        u.id,
        au.id   AS amo_mng_id,
        CASE
            WHEN au.group_id = '296880' THEN 'Line1'
            ELSE 'other'
            END AS group_id,
        u.email
    FROM
        users u                         -- таблица пользователей
        LEFT JOIN amocrm.users au       -- присоединяем таблицу менеджеров из CRM системы, содержащей разделение менеджеров на отделы (группы, group_id)
                  ON au.login = u.email
),
count_intro_lesson AS
-- Группируем по отделам и считаем сумму записей первых и повторных сделанных сотрудниками каждого отдела
(
    SELECT
        m.group_id,
        COUNT(pupil_id) FILTER ( WHERE rn = 1 ) AS zvu1,
        COUNT(pupil_id) FILTER ( WHERE rn > 1 ) AS zvu2
    FROM
        all_intro_lessons ail
        INNER JOIN managers m
                   ON m.id = ail.manager
    GROUP BY 1),
finish_intro_lessons AS
-- в этом блоке считаем успешные события (уроки)
(
    SELECT
        fl.pupil_id,
        fl.lesson_start_time,
        eel.user_id AS manager,
        ROW_NUMBER() OVER (PARTITION BY fl.pupil_id ORDER BY fl.lesson_start_time ) rn   -- нумеруем успешные события на первые и повторные
    FROM
        data.fct_lessons fl
        INNER JOIN events_editing_log eel
                   ON fl.event_id = eel.event_id
        INNER JOIN pupils_with_webinar pww
                   ON pww.id = fl.pupil_id
    WHERE
        fl.valid_event = TRUE
        AND pww.vebinar = 'n'
        AND fl.lesson_state = 'finished'
        AND fl.is_intro_lesson = 1
),
count_finish_intro_lessons AS
-- Группируем по отделам и считаем сумму успешных первых и повторных событий сделанных сотрудниками каждого отдела
(
    SELECT
        m.group_id,
        COUNT(pupil_id) FILTER ( WHERE rn = 1 ) AS fl1,
        COUNT(pupil_id) FILTER ( WHERE rn > 1 ) AS fl2
    FROM
        finish_intro_lessons fil
        INNER JOIN managers m
                   ON m.id = fil.manager
    GROUP BY 1
)
-- собираем все в одну таблицу
SELECT
    cil.group_id          AS "Отделы продаж",
    r.reg                 AS "Регистраций",
    COALESCE(zvu1, 0)     AS "Уникальных записей на ВУ",
    COALESCE(cfil.fl1, 0) AS "Уникальных УВУ"
FROM
    count_intro_lesson cil
    LEFT JOIN count_finish_intro_lessons cfil
              ON cfil.group_id = cil.group_id
    FULL JOIN registrations r
              ON r.group_id = cil.group_id

