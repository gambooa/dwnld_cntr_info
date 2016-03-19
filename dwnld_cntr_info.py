#!/usr/bin/env python
import urllib2
import pymongo
import os
import ConfigParser

DTE_TYPES = [33]
CSV_EMPRESAS = "empresas.csv"
CONTRIBUYENTES_LIMIT = 1
CONTRIBUYENTES_OFFSET = 0
FOLIO_OFFSET = 9
FOLIO_LIMIT = 20

def get_contr():
	contribuyentes_collection = get_collection("contribuyentes")
	contribuyentes = list()
	lines = [line.rstrip('\n') for line in open(CSV_EMPRESAS)]
	for line in lines:
		rut_completo = line.split(';')[0]
		razon_social = unicode(line.split(';')[1], errors='replace')
		rut = rut_completo.split("-")[0]
		dv = rut_completo.split("-")[1]
		contribuyentes.append({"rut": int(rut), "dv": dv, "razon_social": razon_social})
		print "rut: " + rut + ", dv: " + dv, ", razon social: " + razon_social
	result = contribuyentes_collection.insert_many(contribuyentes)

def download_contr():
	contribuyentes_collection = get_collection("contribuyentes")
	contribuyentes = contribuyentes_collection.find({}).limit(CONTRIBUYENTES_LIMIT).skip(CONTRIBUYENTES_OFFSET)
	for key, contribuyente in enumerate(contribuyentes):
		print str(key)  + "/" + str(contribuyentes.count()) + "\t" + str(contribuyente["rut"]) + "-" + str(contribuyente["dv"]) + "\t" + str(contribuyente["razon_social"])
		folder_name = os.path.dirname(os.path.abspath(__file__)) + "/docs/" + str(contribuyente["rut"]) + ""
		check_dir(folder_name)
		folder_name += "/"
		for dte_type in DTE_TYPES:
			folio_from = 1
			folio_to = folio_from + FOLIO_OFFSET
			while folio_from < FOLIO_LIMIT:
				print "T" + str(dte_type) + "\t" + str(folio_from) + '-' + str (folio_to)
				file_name = folder_name + "T" + str(dte_type) + "D" + str(folio_from) + "H" + str(folio_to)
				print file_name
				with open(file_name, "wb") as local_file:
					local_file.write(download_xml(dte_type, contribuyente["rut"], contribuyente["dv"], folio_from, folio_to))
				folio_from += 1 
				folio_from += FOLIO_OFFSET 
				folio_to = folio_from  + FOLIO_OFFSET 

def check_dir(dirname):
	if not os.path.exists(dirname):
		os.makedirs(dirname)

def get_collection(name):
	connection = get_connection()
	collection = connection[name]
	return collection

def get_connection():
	from pymongo import MongoClient
	client = MongoClient('localhost', 27017)
	db = client['test_1']
	return db

def download_xml(dte_type, rut, dv, folio_from, folio_to):
	conf = ConfigParser.ConfigParser()
	conf.read("config.ini")
	dwnld_url = conf.get('URLS', 'dwnld')
	# magic
	url = dwnld_url % (str(rut), str(dv), str(folio_from), str(folio_to), str(dte_type))
	response = urllib2.urlopen(url)
	content = response.read()
	return content

if __name__ == "__main__":
    download_contr()