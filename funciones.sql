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

-- FUNCIÓN PARA COMPLETAR DETALLE Y ACTUALIZAR ORDEN Y STOCK

CREATE OR REPLACE FUNCTION actualizar_datos_pedido()
RETURNS TRIGGER AS $$
DECLARE
    v_precio_producto NUMERIC(10,2);
BEGIN
    -- Obtener precio del producto
    SELECT precio INTO v_precio_producto
    FROM producto
    WHERE id = NEW.id_producto;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Producto con id % no encontrado.', NEW.id_producto;
    END IF;

    -- Calcular precio y monto en el detalle
    NEW.precio := v_precio_producto;
    NEW.monto := NEW.cantidad * v_precio_producto;

    -- Actualizar stock del producto (sumar cantidad)
    UPDATE producto
    SET stock = stock + NEW.cantidad
    WHERE id = NEW.id_producto;

    -- Recalcular monto total de la orden
    UPDATE orden_pedido
    SET monto = (
        SELECT COALESCE(SUM(monto), 0)
        FROM detalle_orden_pedido
        WHERE id_pedido = NEW.id_pedido
    )
    WHERE id = NEW.id_pedido;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER PARA INSERT EN DETALLE_ORDEN_PEDIDO

CREATE TRIGGER trigger_actualizar_datos_pedido
BEFORE INSERT ON detalle_orden_pedido
FOR EACH ROW
EXECUTE FUNCTION actualizar_datos_pedido()

-- VISTA ORDEN_MES_CATEGORIA (solo año más reciente)

CREATE OR REPLACE VIEW orden_mes_categoria AS
SELECT 
    TO_CHAR(op.fecha, 'YYYY-MM') AS mes,
    p.categoria,
    COUNT(DISTINCT op.id) AS "#_ordenes",
    SUM(dop.cantidad) AS total_cantidad,
    ROUND(SUM(dop.monto)::numeric / COUNT(DISTINCT op.id), 2) AS "$_promedio"
FROM orden_pedido op
JOIN detalle_orden_pedido dop ON op.id = dop.id_pedido
JOIN producto p ON dop.id_producto = p.id
WHERE EXTRACT(YEAR FROM op.fecha) = (
    SELECT MAX(EXTRACT(YEAR FROM fecha)) FROM orden_pedido
)
GROUP BY TO_CHAR(op.fecha, 'YYYY-MM'), p.categoria;

-- FUNCIÓN PARA INSERTAR EN ORDEN_MES_CATEGORIA

CREATE OR REPLACE FUNCTION insertar_en_orden_mes_categoria()
RETURNS TRIGGER AS $$
DECLARE
    anio_base INT;
    anio_insert INT := CAST(LEFT(NEW.mes, 4) AS INT);
    mes_insert TEXT := RIGHT(NEW.mes, 2);
    fecha_base DATE := TO_DATE(NEW.mes || '-01', 'YYYY-MM-DD');
    proveedor_id INT := 90;
    producto_id INT;
    precio_unitario NUMERIC;
    i INT;
BEGIN
    -- Validar que sea el año más reciente
    SELECT MAX(EXTRACT(YEAR FROM fecha)) INTO anio_base FROM orden_pedido;
    IF anio_insert <> anio_base THEN
        RAISE NOTICE 'No se puede insertar en años distintos al más reciente (%).', anio_base;
        RETURN NULL;
    END IF;

    -- Validar divisibilidad de cantidad entre órdenes
    IF MOD(NEW.total_cantidad, NEW."#_ordenes") <> 0 THEN
        RAISE NOTICE 'Cantidad total no divisible entre la cantidad de órdenes.';
        RETURN NULL;
    END IF;

    -- Crear producto por default si no existe
    SELECT id INTO producto_id FROM producto
    WHERE descripcion = 'No Asignado - ' || NEW.categoria;

    IF NOT FOUND THEN
        producto_id := (SELECT COALESCE(MAX(id), 89) + 1 FROM producto);
        precio_unitario := NEW."$_promedio" / NEW.total_cantidad;

        INSERT INTO producto(id, descripcion, marca, categoria, precio, stock)
        VALUES (producto_id, 'No Asignado - ' || NEW.categoria, 'NA', NEW.categoria, precio_unitario, NEW.total_cantidad);
    ELSE
        -- Si existe, actualizo el stock
        UPDATE producto
        SET stock = stock + NEW.total_cantidad
        WHERE id = producto_id;

        SELECT precio INTO precio_unitario FROM producto WHERE id = producto_id;
    END IF;

    -- Insertar proveedor si no existe
    IF NOT EXISTS (SELECT 1 FROM proveedor WHERE id = proveedor_id) THEN
        INSERT INTO proveedor(id, cuit, razon_social, tipo_sociedad, direccion, activo, habilitado)
        VALUES (proveedor_id, 0, 'No Asignada', 'SA', '', false, false);
    END IF;

    -- Insertar las órdenes y detalles
    FOR i IN 1..NEW."#_ordenes" LOOP
        INSERT INTO orden_pedido(id, id_proveedor, fecha, monto)
        VALUES (
            (SELECT COALESCE(MAX(id), 300) + 1 FROM orden_pedido),
            proveedor_id,
            fecha_base,
            ROUND(precio_unitario * (NEW.total_cantidad / NEW."#_ordenes"), 2)
        );

        INSERT INTO detalle_orden_pedido(id_pedido, nro_item, id_producto, cantidad)
        VALUES (
            (SELECT MAX(id) FROM orden_pedido),
            1,
            producto_id,
            NEW.total_cantidad / NEW."#_ordenes"
        );
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER INSTEAD OF INSERT EN VISTA ORDEN_MES_CATEGORIA

CREATE TRIGGER trigger_insertar_en_orden_mes_categoria
INSTEAD OF INSERT ON orden_mes_categoria
FOR EACH ROW
EXECUTE FUNCTION insertar_en_orden_mes_categoria();

-- FUNCIÓN PARA BORRAR DESDE LA VISTA ORDEN_MES_CATEGORIA

CREATE OR REPLACE FUNCTION borrar_en_orden_mes_categoria()
RETURNS TRIGGER AS $$
DECLARE
    anio_base INT;
    anio_delete INT := CAST(LEFT(OLD.mes, 4) AS INT);
    fecha_base DATE := TO_DATE(OLD.mes || '-01', 'YYYY-MM-DD');
    producto_id INT;
    cantidad_total INT;
BEGIN
    -- Validar que sea el año más reciente
    SELECT MAX(EXTRACT(YEAR FROM fecha)) INTO anio_base FROM orden_pedido;
    IF anio_delete <> anio_base THEN
        RAISE NOTICE 'No se puede borrar de años distintos al más reciente (%).', anio_base;
        RETURN NULL;
    END IF;

    -- Obtener ID del producto por default
    SELECT id INTO producto_id FROM producto
    WHERE descripcion = 'No Asignado - ' || OLD.categoria;

    IF NOT FOUND THEN
        RAISE NOTICE 'No hay producto por default para categoría %. Nada que borrar.', OLD.categoria;
        RETURN NULL;
    END IF;

    -- Calcular la cantidad total que se va a borrar (para actualizar el stock)
    SELECT COALESCE(SUM(cantidad), 0)
    INTO cantidad_total
    FROM detalle_orden_pedido d
    JOIN orden_pedido o ON d.id_pedido = o.id
    WHERE d.id_producto = producto_id AND o.fecha = fecha_base AND o.id_proveedor = 90;

    -- Borrar detalle y órdenes correspondientes
    DELETE FROM detalle_orden_pedido
    WHERE id_producto = producto_id AND id_pedido IN (
        SELECT id FROM orden_pedido
        WHERE fecha = fecha_base AND id_proveedor = 90
    );

    DELETE FROM orden_pedido
    WHERE fecha = fecha_base AND id_proveedor = 90;

    -- Actualizar stock
    UPDATE producto
    SET stock = stock - cantidad_total
    WHERE id = producto_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER INSTEAD OF DELETE EN VISTA ORDEN_MES_CATEGORIA

CREATE TRIGGER trigger_borrar_en_orden_mes_categoria
INSTEAD OF DELETE ON orden_mes_categoria
FOR EACH ROW
EXECUTE FUNCTION borrar_en_orden_mes_categoria();