import sys
import os

# print(sys.path)
print(os.path.dirname(__file__))
print(os.path.join(os.path.dirname(__file__), '..\..'))
print(os.path.abspath(os.path.join(os.path.dirname(__file__), '..\..')))