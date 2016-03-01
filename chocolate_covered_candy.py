import numpy as np
from numpy.linalg import norm
from numba import jit

def project_onto_ellipsoid(coeffs, size, coords):
  lhs = (coeffs**2).dot(coords**2)
  scale_factor = (size/ lhs)**.5
  return coords * scale_factor

def is_chocolate(coeffs, size, coords):
  proj_point = project_onto_ellipsoid(coeffs, size, coords)
  dist = norm(coords) - norm(proj_point)
#  print norm(coords), norm(proj_point), proj_point
  return (dist <= 1.) and (dist >= 0.) 

a,b= 2., 1.
coeffs = np.array([b, b, a])
size = a**2*b**2

bounds = (size / coeffs**2)**.5 + np.ones(3)
print bounds
nsamples = 10**6
ncor = 0.
print np.prod(bounds) * 8

for i in xrange(nsamples):  
 sample = (np.random.random(3) - .5) * bounds * 2
 if is_chocolate(coeffs, size, sample):
   ncor += 1

print ncor/nsamples, np.pi * 28./3 
print ncor/nsamples * np.prod(bounds) * 8

