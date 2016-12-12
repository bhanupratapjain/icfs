from setuptools import setup

setup(name='Integrated Cloud File System',
      version='0.1',
      description='Integrated Cloud File System',
      url='https://github.ccs.neu.edu/vigneshu/icfs',
      author='bhanupratapjain, sourabhb, vignushu',
      license='MIT',
      include_package_data=True,
      py_modules=['icfs', 'icfs.icfs', 'icfs.pyrsyc', 'icfs.test'],
      install_requires=[
          'Click', 'PyDrive', 'uuid', 'fusepy',
      ],
      entry_points={
          "console_scripts": [
              "icfs = cli:cli",
          ]
      },
      zip_safe=False)
