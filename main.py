import sys
import os

try:
    1/0
except Exception as e:
    print(1)
    sys.exit(1)
else:
    sys.exit(0)