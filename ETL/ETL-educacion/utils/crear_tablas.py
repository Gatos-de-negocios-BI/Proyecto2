def crear_tablas():
    return """
    
        CREATE TABLE IF NOT EXISTS subcategoria(
            subcategoria_key INT PRIMARY KEY,
            subcategoria VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS unidadDeMedida(
            unidadDeMedida_key INT PRIMARY KEY,
            unidadDeMedida VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS indicador(
            indicador_key INT PRIMARY KEY,
            indicador VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS anio(
            anio_key INT PRIMARY KEY,
            descripcion VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS municipio(
            municipio_key INT PRIMARY KEY,
            nombre VARCHAR(30)
        );

        CREATE TABLE IF NOT EXISTS educacion(
            educacion_Key INT PRIMARY KEY,
            Subcategoria_Key INT REFERENCES subcategoria (subcategoria_key),
            UnidadDeMedida_Key INT REFERENCES unidadDeMedida (unidadDeMedida_key),
            Indicador_Key INT REFERENCES indicador (indicador_key),
            Anio_Key INT REFERENCES anio (anio_key),
            Municipio_Key INT REFERENCES municipio (municipio_key),
            datoNumerico DECIMAL
            
        );
    """