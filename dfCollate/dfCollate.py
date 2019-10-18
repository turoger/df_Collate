#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import dask.dataframe as ddf
import pandas as pd
import numpy as np


#
def big_ddf (directory_to_traits_table,
             directory_to_phenotypes, phenotype_list, filename_type,
             directory_to_MAF,
             gene_name, directory_to_rsid):
    '''
    Returns a big dask df with: 'SNP', 'ALLELE', 'iScore', 'BETA', 'PV', 'MAF', 'Gene' as headers
    '''

    trait_dict = ukbbTT_to_dict(directory_to_traits_table)
    pheno_dict = reader(directory_to_phenotypes, phenotype_list, filename_type, trait_dict)
    a_ddf = ddf_collapser(pheno_dict)
    a_ddf = ddf_MAF_Add(directory_to_MAF, a_ddf)
    a_ddf = ddf_Gene_Add(a_ddf, gene_name, directory_to_rsid)
    a_ddf = a_ddf.drop(columns = ['NSE'])

    return(a_ddf)


#
def ukbbTT_to_dict (file_path):
    '''
    Takes a UKBB trait table file path, maps key's to Descriptions and returns a dictionary mapping
    '''

    traits_tb = pd.read_csv(file_path, sep = ',')                                          # read in path of file

    key = traits_tb['key'].tolist()
    val = traits_tb['Description'].tolist()
    key_des_dict = {key[i]:val[i] for i in range(len(key))}                                # creates dictionary

    key_des_dict.update({'21001-0.0':'BMI (kg/m2)',
                        '23104-0.0':'BMI Impedence (kg/m2)',
                        '21002-0.0':'Weight(kg)',
                        '23098-0.0':'Weight Impedence(kg)'})                               # update dict vals with correct descriptors
    return(key_des_dict)


#    
def reader (directory, phenoID_list, filename, trait_dict):
    '''
    Takes a directory of phenotype folders and iterates through the folder's ID in phenoID_list.  
    Inside each folder are potentially four different filename: `imputed.all`, `imputed.norm`, `geno.all`, and `geno.norm`.

    Parameters: 
    directory: the path that holds all the phenotype folders
    phenoID_list: list of phenotype Id's you're interested in collating into a dataframe
    filename: which subset of imputed/genotyped and non-norm/normed files you want to collate
    trait_dict: a dictionary of ID's to Phenotype name mappings

    returns a dictionary based w/ phenoID_list as the key, and the imported dataframe as the value.
    '''

    pheno_ddf_ls = dict()
    os.chdir(directory)

    for i in phenoID_list:
        os.chdir(i)

        for fn in os.listdir():
            if fn.startswith(filename):
                val = ddf.read_csv(fn, sep = ' ',
                                  header = 0,                                               # Ignore initial Headers
                                  names = ['SNP', 'ALLELE', 'iScore', 'BETA', 'NSE', 'PV'], # Specifies Headers
                                  dtype = {'SNP': object,
                                          'ALLELE': object,
                                          'iScore': np.float32,
                                          'BETA': np.float32,
                                          'NSE': np.float32,
                                          'PV': np.float32})                                # Change variable type to save memory
                val['Phenotype'] = trait_dict[i]                                            # Creates new col with name of df as val
                pheno_ddf_ls.update({i:val})

        os.chdir('..')

    return(pheno_ddf_ls)


#
def ddf_collapser(dict_df):
    '''
    Takes dict of ddf and collapses dict into a large ddf. Returns a big dask df
    '''

    frames = []
    for df_name in dict_df.keys():
        frames.append(dict_df[df_name]) #add each df to a list

    return(ddf.concat(frames))


#
def ddf_MAF_Add(file_path, a_ddf):
    '''
    Takes a UKBB MAF txt file for the specified chromosome and cerates a SNP to MAF dict mapping
    Takes a dask df. Creates a new column, MAF, based on k:v pair in snp_dict for ddf in dict
    '''

    mfi_header = ['Loc','SNP', 'Position', 'Allele1', 'Allele2', 'MAF', 'MA', 'Info_score']  # Assigns header to csv
    mfi = pd.read_csv(file_path, names = mfi_header, sep = "\t")                             # Reads tab sep file with names from mfi_header

    key = mfi['SNP'].tolist()
    val = mfi['MAF'].tolist()
    snp_maf_dict = {key[i]:val[i] for i in range(len(key))}                                  # Creates dict mapping for Variant and MAF vals 


    a_ddf['MAF'] = a_ddf['SNP'].map(snp_maf_dict)                                            # Creates new col with MAF mapped to SNP
    a_ddf = (a_ddf
             .assign(MAF = lambda df: df['MAF'].astype(np.float32))                          # Changes df data type for 'MAF'
            )
    return(a_ddf)


#    
def ddf_Gene_Add(a_ddf, gene, rsid_file_path):
    ''' 
    Takes a dask df and sorts by the SNV found in rsid_file_path

    gene: Should be a string to name the values of ddf if the rsid lies within the gene
    rsid_file_path: Should be a directory to a text file with all the variants within a gene

    returns a dask df with in `Gene` or `not Gene`
    '''

    gene_rsid = open(rsid_file_path, 'r')                                                     # read in text file
    gene_rsid_ls = gene_rsid.readlines()                                                      # Add each line as index to a list

    gene_rsid_ls2 = []                                                                        # Strip newline escape, append to list
    for index in range(1, len(gene_rsid_ls)):
        rsid = gene_rsid_ls[index]
        gene_rsid_ls2.append(rsid.rstrip('\n'))

    a_ddf['Gene'] = a_ddf['SNP'].isin(gene_rsid_ls2)                                          # creates a col of T/F depending if in gene_rsid_ls2
    booleanDict = {True: gene, False: 'not_' + gene}                                          # create a dict mapping T/F to a name
    a_ddf['Gene'] = a_ddf['Gene'].map(booleanDict)                                            # change col vals based on mapping

    return(a_ddf)

