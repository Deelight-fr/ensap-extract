#!/usr/bin/python3

import PyPDF2
from os import listdir
from os.path import isfile, join, dirname
from datetime import datetime
import locale
import sqlite3

script_folder = dirname(__file__)

## Configuration

# Dossier contenant les feuilles de paie
fdp_folder = "FDP"

# Base de donnée SQLITE
bdd_file = "fdp.sqlite"

# Creation de la BBD si necessaire
conn = sqlite3.connect(join(script_folder, bdd_file))
cursor = conn.cursor()
sql = 'CREATE TABLE IF NOT EXISTS fdp (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, indice INTEGER, net REAL)'
cursor.execute(sql)
sql = 'CREATE TABLE IF NOT EXISTS fdp_decompte (id INTEGER PRIMARY KEY AUTOINCREMENT, fdp_id INTEGER, code TEXT, libelle TEXT, montant REAL, montant2 REAL, montant3 REAL, FOREIGN KEY(fdp_id) REFERENCES fdp(id))'
cursor.execute(sql)

# Recuperation des FDP
fdp_files = []
for filename in listdir(join(script_folder, fdp_folder)):
	if filename.endswith('.pdf') and '_BP_' in filename:
		fdp_files.append(join(script_folder, fdp_folder, filename))

def extract_data_from_fdp(fdp_file):
	fdp_data = {
		'mois': None,
		'net': None,
		'indice': None,
		'decompte': []
	}
	pdfFileObj = open(fdp_file, 'rb')
	pdfReader = PyPDF2.PdfReader(pdfFileObj)
	pageObj = pdfReader.pages[0]
	lines = pageObj.extract_text("layout").split('\n')

	in_decompte = False

	for idx, line in enumerate(lines):
		if line == '€ € €':
			fdp_data['indice'] = int(lines[idx-1].split(' ')[-1])
			continue
		if in_decompte:
			if line == '€':
				in_decompte = False
				continue
			code = line[0:6]
			libelle = line[6:line.find('€')-1]
			montant = line[line.find('€')+1:].strip().replace(',', '.')
			montant2 = None
			if ' ' in montant:
				# deux montants pour ce decompte
				montant2 = montant[montant.find(' '):].strip().replace(',', '.')
				montant = montant[:montant.find(' ')]
			fdp_data['decompte'].append({
				'code': code,
				'libelle': libelle,
				'montant': montant,
				'montant2': montant2,
#				'raw': line
			})
			continue
		if line.startswith('MOISDE'):
			if 'FEVRIER' in line:
				line = line.replace('FEVRIER', 'FÉVRIER')
			if 'AOUT' in line:
				line = line.replace('AOUT', 'AOÛT')
			if 'DECEMBRE' in line:
				line = line.replace('DECEMBRE', 'DÉCEMBRE')
			locale.setlocale(locale.LC_ALL, 'fr_FR.utf8')
			date_str_fr_FR = line[6:].lower()
			datetime_object = datetime.strptime(date_str_fr_FR, '%B %Y')
			fdp_data['mois'] = datetime_object
			continue
		if line.startswith('NETÀPAYER'):
			fdp_data['net'] = float(line[9:line.find('€')-1].strip().replace(',', '.').replace(' ',''))
		if line.startswith('CODE ÉLÉMENTS'):
			in_decompte = True
			continue
#		print(line)
	
	return(fdp_data)


for fdp_file in fdp_files:
	fdp_data = extract_data_from_fdp(fdp_file)

	# Creation de la FDP en base
	print('Lecture de la feuille de paie %s...' % fdp_data['mois'].strftime('%Y-%m'))
	sql = "SELECT id FROM fdp WHERE date == '%s'" % fdp_data['mois'].strftime('%Y-%m')
	result = cursor.execute(sql)
	if result.fetchone() is None:
		print("\tImport en base de données... ", end="")
		sql = "INSERT INTO fdp (date, indice, net) VALUES (?, ?, ?)"
		result = cursor.execute(sql, (fdp_data['mois'].strftime('%Y-%m'), fdp_data['indice'], fdp_data['net']))
		conn.commit()
		fdp_id = cursor.lastrowid

		# Creation des decomptes en base
		for decompte in fdp_data['decompte']:
			sql = "INSERT INTO fdp_decompte (fdp_id, code, libelle, montant) VALUES (?, ?, ?, ?)"
			result = cursor.execute(sql, (fdp_id, decompte['code'], decompte['libelle'], decompte['montant']))
			conn.commit()
		print("Ok")
	else:
		print("\tDéjà présente en base de données.")

print('Terminé.')
exit()

#sql = "SELECT * FROM fdp ORDER BY date"
#result = cursor.execute(sql)
#all_fdp = result.fetchall()
#print(all_fdp) 
