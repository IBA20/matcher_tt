# Парсер  excel файлов

## Задание

Создать парсер excel файла (см. example.xlsx) на Python.  
Создать таблицу согласно нормам реляционных баз данных (внести все значения в одну таблицу).  
Добавить расчетный тотал по Qoil, Qliq, сгруппированный по датам (даты можете указать свои, добавив программно, не изменяя исходный файл, при условии, что дни будут разные, а месяц и год одинаковые).

## Установка и запуск

1. Клонируйте данный репозиторий.  
2. Создайте виртуальное окружение и установите зависимости:  
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. В корне проекта создайте файл .env со следующим содержимым:  
```
DB_PATH=<путь к директории с базой sqlite>
RAW_DATA_PATH=<путь к директории с excel-файлами для обработки>
```
4. Поместите excel-файлы, подлежащие обработке, в выбранную директорию. Формат файлов должен соответствовать примеру (example.xlsx).  
5. Запустите скрипт командой `python3 parser.py`  
6. Обработанные файлы будут помечены как "_processed" и повторно обрабатываться не будут.  

## Цели проекта

Код представляет собой решение тестового задания компании MATCHER