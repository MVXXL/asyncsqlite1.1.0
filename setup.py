import re
from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    readme = f.read()

version = ''
with open('asyncsqlite/__init__.py', encoding='utf-8') as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE).group(1)

setup(
    name='asyncsqlite',
    version=version,  
    author='MVXXL',
    url='https://github.com/MVXXL/asyncsqlite',  
    packages=find_packages(include=['asyncsqlite', 'asyncsqlite.*']),
    package_data={'asyncsqlite': ['py.typed']},
    license='MIT',
    description='A modern asynchronous SQLite wrapper for efficient database operations in Python.',
    long_description=readme,
    long_description_content_type='text/markdown',
    include_package_data=True,
    python_requires='>=3.8.0',  
    install_requires=[
        
    ],
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',  
        'Topic :: Database :: Database Engines/Servers',
        'Development Status :: 5 - Production/Stable',  
        'Framework :: AsyncIO',  
    ],
    keywords='sqlite asyncio asynchronous database python',  
)