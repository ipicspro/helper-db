from setuptools import setup, find_packages


setup(name='helper-db',
      version='0.1',
      url='https://github.com/ipicspro/helper-db',
      license='MIT',
      author='IS',
      author_email='info@ecommaker.com',
      description='helper-db',
      #packages=find_packages(exclude=['tests']),
      packages=['helper-db'],
      long_description=open('README.md').read(),
      zip_safe=False,
      setup_requires=['nose', 'mysqlclient'],
      test_suite='nose.collector')

