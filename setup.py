from setuptools import setup

setup(name='Integrated Cloud File System',
      version='0.1',
      description='Integrated Cloud File System',
      url='https://github.ccs.neu.edu/vigneshu/icfs',
      author='bhanupratapjain, sourabhb, vignushu',
      license='MIT',
      py_modules=['filesystem/filesystem'],
      install_requires=[
          'Click',
      ],
      entry_points={
          "console_scripts": [
              "filesystem = filesystem:cli",
          ]
      },
      zip_safe=False)
