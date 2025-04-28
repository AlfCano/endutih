## Importar endutih 2023

## Instalar paquetes necesarios
require("librarian") # Usa la función require para cargar el paquete librarian.
shelf (c("rio", "stringr", "tidyr", "tibble", "dplyr")) # Después revisa con el paquete shelf y en su caso instala los paquetes necesarios.

##Crear la función iconv.recursive
# La función "iconv.recursive" fue elaborada por los contribuidores de RKWard, y se encuentra disponible en su menú de importación genérica "Archivo->Importar->Formato de Importación->Importación genérica (basada en rio)"
# Crea la función auxiliar "iconv.recursive" para convertir todas las cadenas a la codificación actual.
iconv.recursive <- function (x, from) { # Asigna la función subsecuente al objeto llamado "iconv.recursive".
	attribs <- attributes (x); # Accede a la lista de los atributos de un objeto.
	if (is.character (x)) {    # Si es de tipo character...
		x <- iconv (x, from=from, to="", sub="") # aplica la función "iconv()" para convertir vectores de caracteres entre codificaciones.
	} else if (is.list (x)) {    # Si es una lista.
		x <- lapply (x, function (sub) iconv.recursive (sub, from)) # Se aplica para cada elemento de la lista con la función "lapply()".
	}
	attributes (x) <- lapply (attribs, function (sub) iconv.recursive (sub, from)) # Crea los atributos para todos los elementos de la lista.
	x
} # Fin de la declaración de la función. 

## Crear ruta de acceso al directorio extraído desde DA
# se debe indicar la dirección en la que se ha extraído el directorio, desde el archivador:  "> ~2017_trim3_enoe_csv"

local({                           # Inicia el ambiente local.
dir  <-  "/directorio/a/carpeta/ENDUTIH/" # Se coloca aquí la dirección al directorio que contiene las carpetas "conjunto de datos_...".
cd   <-  "conjunto_de_datos"      # Cadena constante en los nombres de las tablas.
prog <-  "tr_endutih"                   # Programa de información que se importará.
tip  <- (c("hogares","residentes","usuarios2","usuarios", "viviendas")) # Combina cadenas en una columna (vector) con los nombres de las tablas a importar.
year <-  "anual_2023"                   # Periodo.
fp   <- list()                    # Crea la lista para asignar las rutas a los archivos. siguiente (CONTENEDOR).
for(i in tip) {                   # Inicia el loop con "for(){}", para cada el elemento en el puesto "i" (i-ésimo) en la columna "tip" (SECUENCIA).
tab <- paste(prog, i, year, sep = "_") # Pega los nombres para cada puesto "i" (OPERACIONES).
r   <- file.path (dir, cd, tab, fsep = .Platform$file.sep) # Crea la ruta de archivos con el separador de la plataforma "/" ó "\".
resultado <- paste(prog, year, sep = "_")
fp[[i]] <- r           # Asigna el resultado de la ruta de archivo al ambiente global.
}  # Cierra el loop.
fp$dir <- dir          # Asigna el objeto dir al entorno global. Éste aloja la ruta que servirá para guardar el resultado, la tabla con las tres tablas unidas y sus metadatos.
fp$resultado <- resultado
.GlobalEnv$fp <- fp
}) # Cierra el ambiente local.


## Dirección donde se alojará la tabla al final del guion
# Designación del directorio de trabajo con la función `setwd`
local({
setwd(fp$dir) # Ejecuta la función "setwd". 
})

# ### Diccionarios de datos ###

## Importar el diccionario de datos para tabla todas las tablas
# Designación del directorio de trabajo con la función `setwd` Catálogo de datos: "catalogos".

local({
## Computar 
setwd(file.path(fp$dir,"diccionario_de_datos", fsep = .Platform$file.sep))
}) 

## Importar la lista "dic" para todas las tablas.

local({
## Preparar
library("rio")       # Carga el paquete "rio".
##Computar
sdem_files <- list.files(pattern = "\\.csv$") # Enlista los archivos en directorio con extensión csv.
.GlobalEnv$endutih <- list()        # Crea la lista "endutih" en el Ambiente Global (".GlobalEnv").
.GlobalEnv$endutih$dic <- list()    # Crea la lista "dic" dentro de "endutih" que guardará el resultado del loop.
for (i in sdem_files) {         
data <- import(i)               # Importa los archivos listados en el directorio.
data <- iconv.recursive (data, from= "UTF-8") # Convierte todas las cadenas de "latin1" o "UTF-8" a la actual.
##Asignar el resultado
.GlobalEnv$endutih$dic[[i]] <- data # Asigna el resultado a la lista "dic" en el Ambiente Global dentro de la lista "endutih".
}
})


# Para limpiar los nombres usamos de la cadena ".csv" de "stringr".


## Preparar
library("stringr")
##Computar
names(endutih$dic) <- str_replace(names(endutih$dic), pattern = "_anual_2023.csv", replacement = "") # Para eliminar la cadena ".csv", de los nombres se aplica la función"str_replace()" para reemplazar: ".csv" dentro de los nombres ("names()") de la lista ("endutih$cat") que contiene las tablas correspondientes a los archivos importados y reemplazar por: "", es decir, por cadena vacía.
names(endutih$dic) <- str_replace(names(endutih$dic), pattern = "diccionario_de_datos_tr_endutih_", replacement = "")



## Creación de la lista de data.frame "noms" para seleccionar variables a convertir en factor y creación de la lista cat con las listas de de dataframes de etiquetas de valor para cada una de las 5 tablas de la endutih.

# Se crea un marco de datos para seleccionar las variables cuyos valores deberán ser etiquetados.

local({
  ## Preparar
  library("tidyr")
  library("tibble")
  library("dplyr")
  library("stringr")

  # Lista de nombres de los data.frame a procesar
  nombres_df <- c("residentes", "hogares", "usuarios", "usuarios2", "viviendas")

  # Inicializar las listas para los resultados
  cat_global <- list()
  noms_global <- list()

  ## Iterar sobre cada data.frame
  for (nombre_df in nombres_df) {
    # Obtener el data.frame actual
    res <- endutih$dic[[nombre_df]]

    # Crear el data.frame 'noms' para el data.frame actual
    noms_local <- subset(res, select = c("COLUMNA", "DESCRIPCION", "METADATOS", "TIPO_DATO")) %>%
      filter(!is.na(METADATOS) & str_trim(METADATOS) != "") %>%
      filter(TIPO_DATO != "Numeric") %>%
      rownames_to_column(var = "ID")
      # Modificar la división de METADATOS usando una expresión regular como separador
noms_local <- noms_local %>%
  separate_rows(
    METADATOS,
    sep = ",\\s*(?=\\d)",  # Divide en comas seguidas de espacio opcional y número
    convert = TRUE
  )

    # Identificar las filas donde 'ID'
    repeated_id_indices <- which(duplicated(noms_local$ID) | duplicated(noms_local$ID, fromLast = TRUE))

    # Crear las nuevas columnas inicializadas con NA
    noms_local <- noms_local %>%
      mutate(
        CODIGO = NA_character_,
        ETIQUETA = NA_character_
      )

    # Separar la columna 'METADATOS' para las filas con valores repetidos
    for (i in repeated_id_indices) {
      metadata_value <- noms_local$METADATOS[i]
      # Encontrar la posición del primer espacio después de uno o más dígitos al inicio
      match <- str_match(metadata_value, "^(\\d+)\\s+(.*)")


      if (!is.na(match[1])) {
        noms_local$CODIGO[i] <- match[2] # El segundo grupo de captura son los dígitos
        noms_local$ETIQUETA[i] <- match[3] # El tercer grupo de captura es el resto de la cadena
      } else {
        # Si no hay un patrón de número seguido de espacio, se podría manejar CODIGO como NA y asignar todo a ETIQUETA
        noms_local$ETIQUETA[i] <- metadata_value
      }
    }

   # Identificar los valores repetidos en "ID"
    repeated_ids <- names(which(table(noms_local$ID) > 1))
    # Crear una lista para almacenar los data.frame resultantes para el df actual
    res_local <- list()
    # Iterar sobre los valores repetidos y crear los data.frame
    for (val in repeated_ids) {
      subset_df_id <- noms_local[noms_local$ID == val, ]
      subset_df_col <- noms_local[noms_local$COLUMNA == val, ]
      # Combinar los data.frame encontrados para el valor actual
      combined_df <- unique(rbind(subset_df_id, subset_df_col))
      # Asignar el data.frame a la lista 'res_local' usando el valor de 'COLUMNA' como nombre
      # Tomamos el primer valor de 'COLUMNA' del subset (deberían ser iguales para las repeticiones)
      if (nrow(combined_df) > 0) {
        res_local[[combined_df$COLUMNA[1]]] <- combined_df
      }
    }

    # Asignar los resultados a las listas globales con nombres alusivos
    cat_global[[nombre_df]] <- res_local
    noms_global[[paste0("noms_", nombre_df)]] <- noms_local
  }

  ## Asignar los resultados al entorno global dentro de 'endutih'
  .GlobalEnv$endutih$cat <- cat_global
  .GlobalEnv$endutih$noms <- noms_global
})


## Crear la lista con los datos "data"

local({
## Preparar
library("rio")       # Carga el paquete "rio".
##Computar
setwd(file.path(fp$dir,"conjunto_de_datos", fsep = .Platform$file.sep))
.GlobalEnv$endutih$data <- list()        # Crea la lista "data" en el Ambiente Global en la lista "endutih" (".GlobalEnv").
endutih_data <- list.files(pattern = "\\.csv$") # Enlista los archivos en directorio con extensión csv.
for (i in endutih_data) {
data <- import(i)               # Importa los archivos listados en el directorio.
data <- iconv.recursive (data, from= "UTF-8") # Convierte todas las cadenas de "latin1" o "UTF-8" a la actual.
##Asignar el resultado
.GlobalEnv$endutih$data[[i]] <- data # Asigna el resultado a la lista "dic" en el Ambiente Global dentro de la lista "endutih".
}
})

## Limpriar la lista data
## Preparar
library("stringr")
##Computar
names(endutih$data) <- str_replace(names(endutih$data), pattern = "_anual_2023.csv", replacement = "") # Para eliminar la cadena ".csv", de los nombres se aplica la función"str_replace()" para reemplazar: ".csv" dentro de los nombres ("names()") de la lista ("endutih$cat") que contiene las tablas correspondientes a los archivos importados y reemplazar por: "", es decir, por cadena vacía.
names(endutih$data) <- str_replace(names(endutih$data), pattern = "tr_endutih_", replacement = "")


# asignar etiquetas de valor a la lista data de las variables identificadas como "factor()"

local({
endutih$data <- lapply(names(endutih$data), function(df_name) {
  df_data <- endutih$data[[df_name]]     # Dataframe actual (ej: hogares)
  df_cat <- endutih$cat[[df_name]]       # Lista de categorías correspondiente

  # Iteramos sobre cada columna del dataframe
  for (col_name in names(df_data)) {
    if (col_name %in% names(df_cat)) {   # Si existe en 'cat'
      cat_info <- df_cat[[col_name]]     # Dataframe con niveles y etiquetas

      # Convertimos a factor usando CODIGO y ETIQUETA
      df_data[[col_name]] <- factor(
        df_data[[col_name]],
        levels = cat_info$CODIGO,
        labels = cat_info$ETIQUETA
      )
    }
  }

  return(df_data)  # Devolvemos el dataframe modificado
})
# Asignamos los nombres originales
names(endutih$data) <- c("hogares", "residentes", "usuarios", "usuarios2", "viviendas")
.GlobalEnv$endutih$data <- endutih$data
})

## Ahora se asignan las etiquetas de valor en el formato `RKWard` con la función `rk.set.label()`. Para aplicar la etiqueta correspondiente a cada una de las variables del conjunto de datos, se ha construido un loop "for" con "if".

# Asignación de etiqueta de variable
local({
for (df_name in names(endutih$data)) {
  df <- endutih$data[[df_name]]
  dic_df <- endutih[["dic"]][[df_name]]  # Asume que los nombres coinciden (ej: "hogares" en data y dic)

  if (!is.null(dic_df)) {  # Si existe un diccionario para este data.frame
    for (col_name in names(df)) {
      if (col_name %in% dic_df$COLUMNA) {
        label_text <- dic_df$DESCRIPCION[dic_df$COLUMNA == col_name]
        rk.set.label(df[[col_name]], label_text)
      }
    }
  }
  endutih$data[[df_name]] <- df
}
  .GlobalEnv$endutih$data <- endutih$data
})


## Finalmente Guardar

local({
attach(endutih[["data"]])
## Computar
setwd(fp$dir) # Ejecuta la función "setwd".
archivo <- paste(fp[["resultado"]], "RData", sep = ".") # Crear el nombre del archivo.
save(hogares, residentes, usuarios, usuarios2, viviendas,
	file= archivo)
## Imprimir el resultado
rk.header ("Guardar objetos R", parameters=list(Directory=fp$dir, File=archivo,
	"Objeto"="data"))
detach(endutih[["data"]])
})

