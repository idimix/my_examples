# Проект 1. Примерка одежды

## Дедлайн
8 декабря 2021, пн, 23:59

## Задача

Вы работаете в компании над виртуальной примерочной. Пользователь загружает в примерочную свою фотографию и выбирает понравившуюся одежду. Примерочная отображает, как одежда будет смотреться на пользователе на фотографии. Ваша задача - обучить нейронную сеть, которая будет для изображения пользователя вычислять текстурные координаты, по которым на него примерится одежда.

## Данные

Датасет DensePose для этого проекта состоит из фотографий с людьми и json-файла с разметкой. DensePose - это разметка людей поверх очень общего датасета COCO. В нем очень много фотографий, из которых около 30к фотографий с людьми. Для работы с json-файлами поставляется специальная библиотека https://pypi.org/project/pycocotools/. Пример работы с датасетом можно посмотреть здесь: https://github.com/facebookresearch/DensePose/blob/main/PoseTrack/DensePose-PoseTrack-Visualize.ipynb 

![anno2](https://user-images.githubusercontent.com/800129/141993964-2e6dcfcb-70ac-4e16-911c-c5dc2ecff301.png)

Датасет загружен на [Kaggle.com](https://www.kaggle.com/kapulkin/denseposetrainimages). Его удобно подключить к Python Notebook, созданному на Kaggle.com для работы.

## Библиотека с моделями для сегментации

Можно использовать библиотеку Segmentation Models https://github.com/qubvel/segmentation_models.pytorch с различными архитектурами моделей для сегментации. Для моделей можно выбрать encoder с предобученными весами. Notebook с примером обучения модели и применения аугментации можно посмотреть здесь: https://github.com/qubvel/segmentation_models.pytorch/blob/master/examples/cars%20segmentation%20(camvid).ipynb

### DensePoseLoss

Для Segmentation Models написан совместимый с ней класс DensePoseLoss с loss-ом для обучения [projects/project01/densePoseLoss.py](https://github.com/newprolab/content_deeplearning8/blob/main/projects/project01/densePoseLoss.py). Для его корректной работы нужно установить зависимости: pytorch версии 1.8 и новее, и модули detectron2 и densepose. Модули detectron2 и densepose можно поставить из репозитория:

#### Detectron2:

```
python -m pip install 'git+https://github.com/facebookresearch/detectron2.git'
```

Для Mac OS:

```
CC=clang CXX=clang++ ARCHFLAGS="-arch x86_64" python -m pip install 'git+https://github.com/facebookresearch/detectron2.git'
```

#### Densepose
```
pip install git+https://github.com/facebookresearch/detectron2@main#subdirectory=projects/DensePose
```

#### Другие зависимости

```
pip install UVTextureConverter
```

## Проверка

Проверка будет осуществляться автоматическим чекером со страницы проекта в Личном кабинете.

Чекер будет читать ваш файл с прогнозом, сравнивать с реальным ответом и выдавать вам скор с другой информацией, полезной для дебага, если скор не посчитался.

### Метрика и порог

Вам нужно получить значение mean loss меньше, чем 2.375, чтобы проект был защитан.