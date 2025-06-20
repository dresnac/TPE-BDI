DROP TABLE IF EXISTS detalle_orden_pedido CASCADE;
DROP TABLE IF EXISTS orden_pedido CASCADE;
DROP TABLE IF EXISTS producto CASCADE;
DROP TABLE IF EXISTS proveedor CASCADE;

SET datestyle = 'DMY';

CREATE TABLE proveedor (
    id            INTEGER PRIMARY KEY,
    cuit          BIGINT NOT NULL UNIQUE,
    razon_social  TEXT NOT NULL,
    tipo_sociedad TEXT CHECK (tipo_sociedad IN ('SA', 'SRL', 'SAS', 'Colectiva')),
    direccion     TEXT,
    activo        BOOLEAN NOT NULL,
    habilitado    BOOLEAN NOT NULL
);

CREATE TABLE producto (
    id          INTEGER PRIMARY KEY,
    descripcion TEXT NOT NULL,
    marca       TEXT,
    categoria   TEXT,
    precio      NUMERIC(10, 2) NOT NULL,
    stock       INTEGER NOT NULL
);

CREATE TABLE orden_pedido (
    id           INTEGER PRIMARY KEY,
    id_proveedor INTEGER NOT NULL REFERENCES proveedor(id),
    fecha        DATE NOT NULL,
    monto        NUMERIC(12, 2)
);

CREATE TABLE detalle_orden_pedido (
    id_pedido   INTEGER REFERENCES orden_pedido(id),
    nro_item    INTEGER,
    id_producto INTEGER REFERENCES producto(id),
    cantidad    INTEGER NOT NULL,
    precio      NUMERIC(10, 2),
    monto       NUMERIC(12, 2),
    PRIMARY KEY (id_pedido, nro_item)
);

-- FUNCION PARA COMPLETAR DETALLE Y ACTUALIZAR ORDEN Y STOCK

CREATE OR REPLACE FUNCTION actualizar_datos_pedido()
RETURNS TRIGGER AS $$
DECLARE
    v_precio_producto NUMERIC(10,2);
BEGIN
    SELECT precio INTO v_precio_producto
    FROM producto
    WHERE id = NEW.id_producto;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Producto con id % no encontrado.', NEW.id_producto;
    END IF;

    UPDATE producto
    SET stock = stock + NEW.cantidad
    WHERE id = NEW.id_producto;

    UPDATE detalle_orden_pedido
    SET precio = v_precio_producto,
        monto = NEW.cantidad * v_precio_producto
    WHERE id_pedido = NEW.id_pedido AND nro_item = NEW.nro_item;

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
AFTER INSERT ON detalle_orden_pedido
FOR EACH ROW
EXECUTE FUNCTION actualizar_datos_pedido();

-- VISTA ORDEN_MES_CATEGORIA (solo anio mas reciente)

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

-- FUNCION PARA INSERTAR EN ORDEN_MES_CATEGORIA

CREATE OR REPLACE FUNCTION insertar_en_orden_mes_categoria()
RETURNS TRIGGER AS $$
DECLARE
    anio_base INT;
    anio_insert INT := CAST(LEFT(NEW.mes, 4) AS INT);
    mes_insert TEXT := RIGHT(NEW.mes, 2);
    fecha_base DATE := TO_DATE(NEW.mes || '-01', 'YYYY-MM-DD');
    proveedor_id INT;
    producto_id INT;
    precio_unitario NUMERIC;
    i INT;
BEGIN
    SELECT MAX(id) INTO proveedor_id FROM producto;
    -- Validar que sea el anio mas reciente
    SELECT MAX(EXTRACT(YEAR FROM fecha)) INTO anio_base FROM orden_pedido;
    IF anio_insert <> anio_base THEN
        RAISE NOTICE 'No se puede insertar en anios distintos al mas reciente (%).', anio_base;
        RETURN NULL;
    END IF;

    -- Validar divisibilidad de cantidad entre ordenes
    IF MOD(NEW.total_cantidad, NEW."#_ordenes") <> 0 THEN
        RAISE NOTICE 'Cantidad total no divisible entre la cantidad de ordenes.';
        RETURN NULL;
    END IF;

    -- Crear producto por default si no existe
    SELECT id INTO producto_id FROM producto
    WHERE descripcion = 'No Asignado - ' || NEW.categoria;

    IF NOT FOUND THEN
        producto_id := (SELECT COALESCE(MAX(id), 0) + 1 FROM producto);
        precio_unitario := (NEW."$_promedio" * NEW."#_ordenes") / NEW.total_cantidad;

        INSERT INTO producto(id, descripcion, marca, categoria, precio, stock)
        VALUES (producto_id, 'No Asignado - ' || NEW.categoria, 'NA', NEW.categoria, precio_unitario, 0);
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

    -- Insertar las ordenes y detalles
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

-- FUNCION PARA BORRAR DESDE LA VISTA ORDEN_MES_CATEGORIA

CREATE OR REPLACE FUNCTION borrar_en_orden_mes_categoria()
RETURNS TRIGGER AS $$
DECLARE
    anio_base INT;
    anio_delete INT := CAST(LEFT(OLD.mes, 4) AS INT);
    fecha_base DATE := TO_DATE(OLD.mes || '-01', 'YYYY-MM-DD');
    producto_id INT;
    cantidad_total INT;
BEGIN
    -- Validar que sea el anio mas reciente
    SELECT MAX(EXTRACT(YEAR FROM fecha)) INTO anio_base FROM orden_pedido;
    IF anio_delete <> anio_base THEN
        RAISE NOTICE 'No se puede borrar de anios distintos al mas reciente (%).', anio_base;
        RETURN NULL;
    END IF;

    -- Obtener el ID del producto correspondiente al mes y categoria
    SELECT DISTINCT p.id
    INTO producto_id
    FROM producto p
    JOIN detalle_orden_pedido d ON d.id_producto = p.id
    JOIN orden_pedido o ON o.id = d.id_pedido
    WHERE TO_CHAR(o.fecha, 'YYYY-MM') = OLD.mes
     AND p.categoria = OLD.categoria;

    IF NOT FOUND THEN
     RAISE NOTICE 'No se encontro ningun producto para mes % y categoria %.', OLD.mes, OLD.categoria;
    RETURN NULL;
    END IF;

    -- Calcular la cantidad total que se va a borrar
    SELECT COALESCE(SUM(cantidad), 0)
    INTO cantidad_total
    FROM detalle_orden_pedido d
    JOIN orden_pedido o ON d.id_pedido = o.id
    WHERE d.id_producto = producto_id AND o.fecha = fecha_base;

    -- Borrar detalle y ordenes correspondientes
    DELETE FROM detalle_orden_pedido
    WHERE id_producto = producto_id AND id_pedido IN (
        SELECT id FROM orden_pedido
        WHERE fecha = fecha_base
    );

    DELETE FROM orden_pedido
    WHERE fecha = fecha_base;

    -- Actualizar stock
    UPDATE producto
    SET stock = stock - cantidad_total
    WHERE id = producto_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_borrar_en_orden_mes_categoria
INSTEAD OF DELETE ON orden_mes_categoria
FOR EACH ROW
EXECUTE FUNCTION borrar_en_orden_mes_categoria();
