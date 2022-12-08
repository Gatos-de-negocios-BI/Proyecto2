def crear_tablas():
    return """

        CREATE TABLE IF NOT EXISTS anio(
            anio_key INT PRIMARY KEY,
            descripcion VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS municipio(
            municipio_key INT PRIMARY KEY,
            nombre VARCHAR(30)
        );

        CREATE TABLE IF NOT EXISTS subcategoria(
            subcategoria_key INT PRIMARY KEY,
            subcategoria VARCHAR(150)
        );
    
        CREATE TABLE IF NOT EXISTS poblacion(
            Poblacion_Key INT PRIMARY KEY,
            Anio_Key INT REFERENCES anio (anio_key),
            Municipio_Key INT REFERENCES municipio (municipio_key),
            Subcategoria_Key INT REFERENCES subcategoria (subcategoria_key),
            poblacion DECIMAL
        );


    """