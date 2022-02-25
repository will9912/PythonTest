import pandas as pd
import sqlite3

csv_path = "clientes.csv"
output_path = "output/"
df_clientes = pd.read_csv(csv_path, sep=';')

# convertimos los tipos de datos a string, int64, y fechas a datetime64
df_clientes = df_clientes.convert_dtypes()
df_clientes["fecha_nacimiento"] = pd.to_datetime(df_clientes["fecha_nacimiento"], format = "%Y-%m-%d")
df_clientes["fecha_vencimiento"] = pd.to_datetime(df_clientes["fecha_vencimiento"], format = "%Y-%m-%d")

# Normalizamos los datos en mayúsculas:
df_clientes = df_clientes.applymap(lambda s: s.upper() if type(s) == str else s)

## Creo las distintas tablas a partir de los datos ya tipados.
# Empezando por el archivo clientes.xlsx, calculamos age y age_group
tabla_clientes = df_clientes
now = pd.Timestamp('now')
tabla_clientes['age'] = (now - tabla_clientes['fecha_nacimiento']).astype('<m8[Y]')
tabla_clientes = tabla_clientes.convert_dtypes()

# NOTA IMPORTANTE ----- No pude realizar columna age_group por errores: 
# ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().
# valueError: invalid entry 0 in condlist: should be boolean ndarray
"""
filters = [
   (tabla_clientes.age <= 20), #primera forma df.column
   (tabla_clientes["age"] > 20) & (tabla_clientes["age"] <= 30), #segunda forma df["age"], sin exito en ambas.
   (tabla_clientes["age"] > 30) & (tabla_clientes["age"] <= 40),
   (tabla_clientes["age"] > 40) & (tabla_clientes["age"] <= 50),
   (tabla_clientes["age"] > 50) & (tabla_clientes["age"] <= 60),
   (tabla_clientes["age"] > 60),
]
values = ["1","2","3","4","5","6"]
tabla_clientes["age_group"] = np.select(filters, values)
"""
# POR ELLO, PROCEDO SIN LA COLUMNA AGE_GROUP

# Calculo la delincuencia, (fecha actual - fecha de vencimiento) en días
tabla_clientes['delincuency'] = (now - tabla_clientes['fecha_vencimiento']).astype('<m8[D]')
tabla_clientes = tabla_clientes.convert_dtypes()
# CAMBIANDO NOMBRE DE COLUMNAS PARA TABLA_CLIENTES
tabla_clientes = tabla_clientes.rename(columns={'fecha_nacimiento':'birth_date', 'fecha_vencimiento':'due_date', 'deuda': 'due_balance', 'direccion': 'address'})
# ELIMINANDO COLUMNAS NO NECESARIAS PARA TABLA DE CLIENTES
tabla_clientes = tabla_clientes.drop(columns=["altura", "peso", "correo", "estatus_contacto", "prioridad", "telefono"])
tabla_clientes.columns
# EXPORTANDO TABLA_CLIENTES A XLSX CARPETA OUTPUT
tabla_clientes.to_excel(output_path+"clientes.xlsx")

## CREANDO TABLA EMAILS

columnas_tabla_emails = [df_clientes["fiscal_id"],df_clientes['correo'],df_clientes['estatus_contacto'],df_clientes['prioridad']]
headers = ["fiscal_id",'email','status','priority']
tabla_emails = pd.concat(columnas_tabla_emails, axis=1, keys=headers)
# EXPORTANDO TABLA_EMAILS A XLSX CARPETA OUTPUT
tabla_emails.to_excel(output_path+"emails.xlsx")

## CREANDO TABLA PHONES

columnas_tabla_phones = [df_clientes["fiscal_id"],df_clientes['telefono'],df_clientes['estatus_contacto'],df_clientes['prioridad']]
headers = ["fiscal_id",'phone','status','priority']
tabla_phones = pd.concat(columnas_tabla_phones, axis=1, keys=headers)
# EXPORTANDO TABLA_PHONES A XLSX CARPETA OUTPUT
tabla_phones.to_excel(output_path+"phones.xlsx")

# LEYENDO ARCHIVOS XLSX E IMPORTÁNDOLOS A BASE DE DATOS

cxn = sqlite3.connect('database.db3')

customers = pd.read_excel(output_path+'clientes.xlsx')
customers.to_sql(name='customers',con=cxn,if_exists='replace',index=True)

emails = pd.read_excel(output_path+'emails.xlsx')
emails.to_sql(name='emails',con=cxn,if_exists='replace',index=True)

phones = pd.read_excel(output_path+'phones.xlsx')
phones.to_sql(name='phones',con=cxn,if_exists='replace',index=True)

cxn.commit()
cxn.close()