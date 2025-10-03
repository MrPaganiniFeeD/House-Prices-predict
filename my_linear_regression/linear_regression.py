import numpy as np
from .losses import BaseLoss
from typing import List, Iterable


def gradient_descent(w_init: np.ndarray, X: np.ndarray, y: np.ndarray, 
                    loss: BaseLoss, lr: float, n_iterations: int = 100000) -> List[np.ndarray]:
    """
    Функция градиентного спуска
    :param w_init: np.ndarray размера (n_feratures,) -- начальное значение вектора весов
    :param X: np.ndarray размера (n_objects, n_features) -- матрица объекты-признаки
    :param y: np.ndarray размера (n_objects,) -- вектор правильных ответов
    :param loss: Объект подкласса BaseLoss, который умеет считать градиенты при помощи loss.calc_grad(X, y, w)
    :param lr: float -- параметр величины шага, на который нужно домножать градиент
    :param n_iterations: int -- сколько итераций делать
    :return: Список из n_iterations объектов np.ndarray размера (n_features,) -- история весов на каждом шаге
    """
    W = []

    for i in range(n_iterations):
        w_init_new = w_init - lr*loss.calc_grad(X, y, w_init)
        W.append(w_init_new)
        w_init = w_init_new
    return W


class LinearRegression1:
    def __init__(self, loss: BaseLoss, lr: float = 0.1) -> None:
        self.loss = loss
        self.lr = lr
        self.w = None
        self.g = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> 'LinearRegression':
        y = np.asarray(y)

        #Your code
        X = np.hstack([X, np.ones([X.shape[0], 1])])
        shapeX = X.shape

        self.w = np.arange(shapeX[-1])
        self.g = gradient_descent(self.w, X, y, self.loss, lr = self.lr, n_iterations = 100000)
        return self.g[-1]

    def predict(self, X: np.ndarray) -> np.ndarray:
        # Проверяем, что регрессия обучена, то есть, что был вызван fit и в нём был установлен атрибут self.w
        assert hasattr(self, "w"), "Linear regression must be fitted first"
        assert hasattr(self, "g"), "Linear regression must be fitted first"
        #Your code
        X = np.hstack([X, np.ones([X.shape[0], 1])])
        y = np.dot(X, self.g[-1])
        return y

print('hello world')