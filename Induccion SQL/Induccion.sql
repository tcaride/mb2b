1- Tenemos las bases de datos que estan alojados en un servidor. Nosotros tenemos 2 servidores principalmente: BigQuery y Teradata.

2- Para acceder al servidor necesitamos un cliente. En MELI usamos: Gaia compose, Alation o Google console.

3- Las bases de datos contienen esquemas (schemas), el esquema central que vamos a utilizar en MELI se llama WHOWNER (tanto en Teradata como en BigQuery)

4- Nuestro esquema de B2B esta solo en BigQuery y se llama SBOX_B2B_MKTPLACE

5- Los esquemas contienen tablas y las tablas contienen campos (columnas) y registros (filas).

6- Cada campo contiene el mismo tipo de formato. O texto, o numero, o fecha. Pero un campo no puede tener algunos registros en texto y otros en numeros.

7- Tenemos 4 tipos de consultas pero vamos a usar mayoritariamente 2: DML (Data Manipulation Language) & DDL (Data Definition Language)

8- Dentro de DML tenemos: SELECT, INSERT, UPDATE, DELETE. Select es la que mas vamos a usar, las otras quedan para el final como extra.


---------------------------------------------------
9- La consulta SELECT tiene como forma sintactica:

SELECT
<columnas>
FROM 
<nombre_tabla>

10- Para traer todos los valores usamos el caracter "*""

11- Para traer algun campo escribimos el nombre entre SELECT y FROM. Cada campo va separado con ","" excepto el ultima. Al igual que cuando guardamos un archivo csv.

SELECT
campo_1, campo_2
FROM 
<nombre_tabla>

12- Podemos renombrar columnas en el resulado escribiendo el nombre despues del campo o colocando "as" entre el campo y el nombre:

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>

13- Para limitar los resultados de la consulta podemos usar LIMIT en Bigquery y SAMPLE en teradata. Escribimos al finalizar la consulta el LIMIT con el numero de registros que queremos renderizar. La consulta va a correr y luego va a mostrar solo el numero de registros indicado. El limit siempre va al final de todo!

SELECT
*
FROM 
<nombre_tabla>
LIMIT 678

14- ORDENAR: Para ordenar resultados usamos la clausula ORDER BY. Puede ser descendiente DESC o asendente ASC. 

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
ORDER BY campo_1 DESC

15- Podemos hacer ordeners multiples, ordenando primero por una columna y despues por otra. Esos ordenes pueden a su vez ser distintos, uno ascendente y otro descendente. Entre una columna y otra dentro del orden va las "," al igual que como seleccionamos las columnas. A su vez debemos colocar el nombre original del campo ya que aun no lo renombramos (esto no es para todos los servidores asi, pero se puede romper segun el servidor que usemos)

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
ORDER BY campo_1 DESC, campo_2 ASC

---------------------------------------------------

16- Filtrar: Para filtrar resultado usamos la clausula where. El WHERE va despues del nombre de la tabla seleccionada y debemos indicarle la condicion a cumplir por ese campo. Los numeros van sin comilla y los textos y fechas van entre comillas simples.

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1= 343
ORDER BY campo_1 DESC, campo_2 ASC

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1= '2021-08-31'
ORDER BY campo_1 DESC, campo_2 ASC


SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1= 'MLA'
ORDER BY campo_1 DESC, campo_2 ASC

17- Filtrar con condiciones multiples: Utilizamos AND u OR. Tambien podemos usar parentesis para construir las condiciones. Siempre la condicion tiene que tener el campo, el operador logico y el valor buscado.

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1= 'MLA' AND campo_2= 343
ORDER BY campo_1 DESC, campo_2 ASC

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1= 'MLA' OR campo_1= 'MLB' 
ORDER BY campo_1 DESC, campo_2 ASC

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE (campo_1= 'MLA' OR campo_1= 'MLB') AND campo_2= '2021-08-31'
ORDER BY campo_1 DESC, campo_2 ASC

18- Los filtros pueden ser. Like sirve para buscar adentro de un texto, no lo vamos a ver pero saber que esta. 

< Menor que
> Mayor que
<> Distinto de
<= Menor o igual que
>= Mayor o igual que
BETWEEN Intervalo
LIKE Comparacion
In Especificar

Cuando usamos IN debemos construir un array con los valores posibles. Un array se construye con un parentesis y cada valor separado por ","

SELECT
campo_1 renombre_1, campo_2 as renombre_2
FROM 
<nombre_tabla>
WHERE campo_1 IN ('MLA','MLB')
ORDER BY campo_1 DESC, campo_2 ASC

---------------------------------------------------

DISTINCT
FUNCIONES DE AGREGACION
CASE WHEN

mayuscula o minuscula
comentarios
