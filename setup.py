from distutils.core import setup

setup(
        name='ukbb_scripts',
        version='0.0.0',
        scripts='df_Collate',
        author='Roger Tu',
        author_email='rogertu@scripps.edu',
        url='https://github.com/turoger/df_Collate',
        description='Scripts for working with gene atlas data',
        license='BSD3',
        keywords='gene atlas',
        classifiers=[
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'License :: OSI Approved :: BSD3 License',
            'Programming Language :: Python :: 3',
            ],
        packages=['df_Collate',],
        )

