from setuptools import setup, find_namespace_packages

setup(
    name='dbt-reshaper',
    version='1.0',
    python_requires='>=3.10',
    install_requires=['dbt-core >= 1.3.0', 'boto3'],
    extra_requires=[],
    packages=find_namespace_packages(include=["dbt", "dbt.*","dbt_reshaper", "dbt_reshaper.*", "glueduck", "glueduck.*"]),
    include_package_data=True,
    package_data={
        'dbt_reshaper': [
            'dbt_project/**',
        ]
    }
)
