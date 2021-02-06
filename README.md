# Добавление поддержки SQL-подобного языка для потоковой обработки данных в Apache Flink
В рамках нашего проекта мы осуществили интеграцию инструментов для потоковой обработки данных Siddhi и Apache Flink. Наше решение позволяет использовать возможности Apache Flink с наименьшим количеством правок в коде для Siddhi.

### Реализованные операторы
* *JOIN*
* *GROUP BY*

### Архитектурное решение
![Архитектура Fliddhi](https://trello-attachments.s3.amazonaws.com/6017f11800d83e7c23a4a55e/974x1136/03c4ece37e1182df4086e0eb2f78171e/fliddhi_arch_v3.jpg "Fliddhi")

## Пример использования
```
// Инициализация DataStream<Row> на которых будет выполняться запрос
   DataStream<Row> sourceStream = env.fromElements(new Object[]{"Vasya", 5f, 50L}, new Object[]{"Lena", 5f, 30L}).map(Row::of);

// Определяем Siddhi query, который необходимо запустить
        String querySelect = "" +
                "define stream StockStream (name string, department float, salary long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select department, name, salary " +
                "insert into OutputStream;";

// Инициализируем среду выполнения нашей библиотечки
        FliddhiExecutionEnvironment fEnv = FliddhiExecutionEnvironment.getExecutionEnvironment(env);
// Регистрируем в нашей среде выполнения потоки (их может быть больше одного) как (Название, сам поток)
        fEnv.registerInputStream("StockStream", sourceStream);
// Выполняем запрос на зарегистрированных потоках, получаем пару key-value, где key -- название выходного потока, dataStream<row> -- сам поток
        Map<String, DataStream<Row>> outputStream = fEnv.siddhiQL(querySelect);
```

## О проекте и о нас
Проект создан в рамках Зимней Школы CompTech 2021 по заказу от компании Huawei.
Более подробно с техническим заданием можно ознакомиться здесь:

https://docs.google.com/document/d/1JTnql3-7uOlBwvzu6ZkPxqVtpWKOAwVTsAv0NfaAbcs/

Наша команда:
+ Безверхова Ольга	*(Technical Writer)*	@brookoli
+ Губаренко Антонина	*(Tester)*	@a-gubarenko
+ Зозуля Артём	*(Developer)*	@Br0adSky
+ Обухова Алиса	*(Teamleader, Developer)*	@lbdlbdlbdl
+ Попов Дмитрий	*(Developer)*	@Popov-Dmitry
+ Трушев Александр	*(Supervisor)*	@trushev
