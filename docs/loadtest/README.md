# Сырые данные нагрузочных прогонов

Это исходные данные для таблиц в документах, читать их подряд не нужно: выводы и методика
описаны там, куда ведут ссылки. JSON пишет бот (`go run ./cmd/bot -json файл`), таблицы
из них строит `cmd/loadreport`; `console-*.txt` — вывод консоли тех же прогонов.

| Файлы | Что за прогон | Где разобран |
|---|---|---|
| `ramp-*.json`, `console-ramp.txt` | Ступенчатая нагрузка, 0–3000 пар | [LOAD_TESTING.md](../LOAD_TESTING.md#ступенчатая-нагрузка) |
| `soak.json`, `console-soak.txt` | 1000 пар × 5 минут | [LOAD_TESTING.md](../LOAD_TESTING.md#soak-тест) |
| `storm.json` | Шторм переподключений | [LOAD_TESTING.md](../LOAD_TESTING.md#шторм-переподключений) |
| `hardening/` | Версии до и после укрепления | [LOAD_TESTING.md](../LOAD_TESTING.md#сравнение-до-и-после-укрепления) |
| `wire/` | JSON и бинарный протокол | [WIRE.md](../WIRE.md#под-нагрузкой) |
| `cluster/` | Три узла, узел остановлен под нагрузкой (SIGKILL и SIGTERM) | [CLUSTER.md](../CLUSTER.md#демо-узел-убит-под-нагрузкой) |
| `heartbeat/` | Ping раз в 20 с и раз в 2 с | [HARDENING.md](../HARDENING.md#стоимость-пингов-раз-в-2-с) |
