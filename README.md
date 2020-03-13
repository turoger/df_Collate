# This is a work in progress.
1. Dataframe Collater Function
This repository uses dask to work through large UK Biobank Summary Statistics.  Given a directory of phenotypes from UK Biobank, it will iterate through the directorys to assemble a large dataframe.

2. Description 
	a. Overview
 	b. Motivation
 	c. Features
 	d. Re-use and contributions statement 

3. Getting Started
 	a. Requirements
		* clone the repository and run ...
 	b. Dependencies
 	c. Step-by-step installation
 	d. Unit tests

4. Usage
 	a. How to use
		* `big_ddf` is the main function. To run this function, you must know the directory of the traits table, the directory to phenotype folders, the total number of phenotypes, the file name types, directory to the MAF files, gene names and directory to the variants (rsids).
		* `ukbbTT_to_dict` takes a trait table file path, and maps key's to descriptions. This returns a dictionary mapping.
		* `reader` takes a directory of phenotype folders and iterates through the folder ID in the phenotype ID list.  Inside each folder are potentially four different file names: "imputed.all", "imputed.norm", "geno.all", and "geno.norm".  You'll need the path that holds all the phenotype folders, a list of phenotype ID's you're interested in collating into a dataframe, a subset of imputed/genotyped and non-norm/normed files you want to collate and a dictionary of IDs to Phenotype name mappings.
		* `ddf_collapser` takes a dictionary of dask dataframes generated and colapses dictionary into one large dask dataframe.
		* `ddf_MAF_Add` takes a UK Biobank MAF txt file for specified chromosome and creats a SNP to MAF dictionary mapping.  It then takes a dask dataframe and appends a new column called MAF based on previous key value pairing.
		* `ddf_Gene_Add` takes a dask dataframe and sorts by the SNV found in rsid_file path.  The gene should be astring to name the value of the dask dataframe., rsid file path should be a directory to text file with all the variants within a gene. It will return a dask datframe of Genes or not Genes of interest.
 	b. Code examples
		* WIP	
5. Project Status
 	a. Current status: WIP
		
6. Contributions
 	a. Contribution Status
 	b. Contribution Guide
 	c. Coding Styles
	d. Contributors 
	e. Acknowledgments
 	f. Licensing
		License: BSD3
