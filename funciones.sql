-- funciones.sql

-- DROP TABLES (para facilitar pruebas durante desarrollo)
DROP TABLE IF EXISTS detalle_orden_pedido CASCADE;
DROP TABLE IF EXISTS orden_pedido CASCADE;
DROP TABLE IF EXISTS producto CASCADE;
DROP TABLE IF EXISTS proveedor CASCADE;

-- Tabla PROVEEDOR
CREATE TABLE proveedor (
    id            INTEGER PRIMARY KEY,
    cuit          BIGINT NOT NULL UNIQUE,
    razon_social  TEXT NOT NULL,
    tipo_sociedad TEXT CHECK (tipo_sociedad IN ('SA', 'SRL', 'SAS', 'Colectiva')),
    direccion     TEXT,
    activo        BOOLEAN NOT NULL,
    habilitado    BOOLEAN NOT NULL
);

-- Tabla PRODUCTO
CREATE TABLE producto (
    id          INTEGER PRIMARY KEY,
    descripcion TEXT NOT NULL,
    marca       TEXT,
    categoria   TEXT NOT NULL,
    precio      NUMERIC(10, 2) NOT NULL,
    stock       INTEGER NOT NULL
);

-- Tabla ORDEN_PEDIDO
CREATE TABLE orden_pedido (
    id           INTEGER PRIMARY KEY,
    id_proveedor INTEGER NOT NULL REFERENCES proveedor(id),
    fecha        DATE NOT NULL,
    monto        NUMERIC(12, 2)
);

-- Tabla DETALLE_ORDEN_PEDIDO
CREATE TABLE detalle_orden_pedido (
    id_pedido   INTEGER REFERENCES orden_pedido(id),
    nro_item    INTEGER,
    id_producto INTEGER REFERENCES producto(id),
    cantidad    INTEGER NOT NULL,
    precio      NUMERIC(10, 2),
    monto       NUMERIC(12, 2),
    PRIMARY KEY (id_pedido, nro_item)
);

-- IMPORTACIÓN DE DATOS

-- IMPORTAR PROVEEDOR
COPY proveedor(id, cuit, razon_social, tipo_sociedad, direccion, activo, habilitado)
FROM 'data/proveedor.csv'
DELIMITER ',' CSV HEADER;

-- IMPORTAR PRODUCTO
COPY producto(id, descripcion, marca, categoria, precio, stock)
FROM 'data/producto.csv'
DELIMITER ',' CSV HEADER;

-- IMPORTAR ORDEN_PEDIDO (sin el campo monto)
COPY orden_pedido(id, id_proveedor, fecha)
FROM 'data/orden_pedido.csv'
DELIMITER ',' CSV HEADER;

-- IMPORTAR DETALLE_ORDEN_PEDIDO (sin los campos precio y monto)
COPY detalle_orden_pedido(id_pedido, nro_item, id_producto, cantidad)
FROM 'data/detalle_orden_pedido.csv'
DELIMITER ',' CSV HEADER;