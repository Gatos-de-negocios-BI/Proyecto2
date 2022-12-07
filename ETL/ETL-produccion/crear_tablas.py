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

        CREATE TABLE IF NOT EXISTS produccionMineraDeMetales(
            Order_Key INT PRIMARY KEY,
            municipio_key INT REFERENCES municipio (municipio_key),
            anio_key INT REFERENCES anio (anio_key),
            produccion_total DECIMAL,
            recurso_natural VARCHAR(10)
        );
    """