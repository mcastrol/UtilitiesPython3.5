# -*- coding: utf-8 -*-
"""mso-distribuciones.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/17u2gtcxFEM5cwDe9mO4nYvo1azF_Ft2z
"""

import numpy as np
#https://numpy.org/doc/1.16/reference/routines.random.html

#generate random between 0,1 - paraam: dmension
x = np.random.rand()
print(x)

#test add dimensions as parameters: 3 luego pasarle 3,5 ¿que obtengo?
x = np.random.rand(3)

#que pasa si necesito números de 0 a 7?
x = np.random.rand(100)*10 % 7
print(x)
print(min(x))
print(max(x))

#que pasa si necesito enteros
#100 tiradas de dado
x = np.random.randint(1,7, size=500)
print(x)
plt.hist(x)

plt.hist(x,density=True,cumulative=True)

#que numero salio más veces? hacemos un histoograma
import matplotlib.pyplot as plt
#https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.html
plt.hist(x,bins=7)

#generar muestras de la llegada a una cola con lambda=5 personas/u.tiempo
x=np.random.poisson(lam=5, size=10)
print(x)
plt.hist(x,bins=15)

#exponencial
#distribucion exponencial. Tiempo entre las llegadas de las personas a una cola con 1/lambda=1/5
x=np.random.exponential(scale=1/5, size=100)
print(x)
print(min(x))
print(max(x))
plt.hist(x,bins=15)

#idead
x = np.random.choice([3, 5, 7, 9], size=100)
print(x)
plt.hist(x)

#idead
x = np.random.choice(["a","b", "c", "d"],  size=100, p = [0.4,0.3,0.2,0.1])
print(x)
plt.hist(x)
