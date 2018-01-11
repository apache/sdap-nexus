

import numpy as np

A = np.arange(12).reshape(3, 4)
b = np.arange(3).reshape(1, 3)

# np.linalg.lstsq(A,b)
# This gives "LinAlgError: Incompatible dimensions" exception

print np.linalg.lstsq(A, b.T)
