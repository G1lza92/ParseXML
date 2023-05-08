import csv
import multiprocessing
import os
import random
import string
import typing
import uuid
from typing import Dict, List
from xml.etree.ElementTree import Element, SubElement, parse, tostring
from zipfile import ZipFile

XML_COUNT = 100
ZIP_COUNT = 50
PATH = '/home/user/Dev/parse_xml/archives' #путь для создания и обработки xlm файов


def random_string(length: int):
    """Генерация случайнойной строки."""
    return ''.join(random.choice(string.ascii_letters) for i in range(length))


def create_xml():
    """Создание xml документа.

    Документ с данными вида:
    <root>
        <var name=’id’ value={id}/>
        <var name=’level’ value={level}/>
        <objects>
            <object name={object}/>
            <object name={object}/>
            ...
        </objects>
    </root>
    """
    root = Element('root')
    var_id = SubElement(
        root, 'var', {'name': 'id', 'value': str(uuid.uuid4())},
    )
    var_level = SubElement(
        root, 'var', {'name': 'level', 'value': str(random.randint(1, 100))},
    )
    objects = SubElement(root, 'objects')
    for i in range(random.randint(1, 10)):
        object = SubElement(objects, 'object', {'name': random_string(10)})
    return tostring(root)


def create_zip_file(path: str, xml_count: int):
    """Генерация zip архива с xml документами.

    path: str - путь до архива.
    xml_count: int - кол-во xml документов в архиве.
    """
    with ZipFile(path, 'w') as archive:
        for i in range(xml_count):
            xml_data = create_xml()
            filename = f'{i + 1}.xml'
            archive.writestr(filename, xml_data)


def first_task(path: str, zip_count: int):
    """Выполнение первого задания.

    path: str - путь до папки в которой нужно сохранить архивы.
    zip_count: int - количество генерируемых архивов.
    """
    for i in range(zip_count):
        archive_name = f"archive_{i+1}.zip"
        zip_path = os.path.join(path, archive_name)
        create_zip_file(zip_path, xml_count=XML_COUNT)
    print('Archives created')


def process_xml_file(xml_file: typing.IO) -> Dict[str, List[tuple]]:
    """Обработка xml файла и получение данных из него.

    xml_file: - файл xml формата

    return: словарь с данными из xml файла.
    """
    result = {'vars': [], 'objects': []}
    tree = parse(xml_file)
    root = tree.getroot()
    var_id = root.find('var[@name="id"]').get('value')
    var_level = root.find('var[@name="level"]').get('value')
    result['vars'].append((var_id, var_level))
    for obj in root.find('objects').findall('object'):
        object_name = obj.get('name')
        result['objects'].append((var_id, object_name))
    return result


def process_zip_file(path: str) -> Dict[str, List[tuple]]:
    """Обработка zip архива.

    path: str - путь до папки с архивами.
    return: словарь с данными из xml файлов.
    """
    result = {'vars': [], 'objects': []}
    with ZipFile(path, 'r') as archive:
        for file in archive.namelist():
            with archive.open(file, 'r') as xml_file:
                res = process_xml_file(xml_file)
                result['vars'] += res['vars']
                result['objects'] += res['objects']
    return result


def second_task(path: str):
    """Выполнение второго задания.

    path: str - путь до папки с архивами.
    """
    file_list = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.zip')]
    results = []
    with multiprocessing.Pool() as pool:
        results = pool.map(process_zip_file, file_list)
        vars = []
        objects = []
        for result in results:
            vars += result['vars']
            objects += result['objects']
    vars_csv = os.path.join(path, 'vars.csv')
    objects_csv = os.path.join(path, 'objects.csv')
    with open(vars_csv, 'w') as vars_file:
        csvwriter = csv.writer(vars_file)
        csvwriter.writerow(('id', 'level'))
        for line in vars:
            csvwriter.writerow(line)
    with open(objects_csv, 'w') as objects_file:
        csvwriter = csv.writer(objects_file)
        csvwriter.writerow(('id', 'object'))
        for line in objects:
            csvwriter.writerow(line)
    print('Archives parsed')


first_task(path=PATH, zip_count=ZIP_COUNT)
second_task(PATH)
