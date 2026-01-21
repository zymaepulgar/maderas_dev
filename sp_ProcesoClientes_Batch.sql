CREATE PROCEDURE dbo.sp_ProcesoClientes_Batch
    @BatchSize INT = 10000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- Si hay error, revienta la transacción

    DECLARE @FilasProcesadas INT = 1;

    BEGIN TRY

        WHILE @FilasProcesadas > 0
        BEGIN
            BEGIN TRAN;

                ;WITH CTE AS
                (
                    SELECT TOP (@BatchSize) IdCliente
                    FROM Clientes WITH (ROWLOCK, READPAST)
                    WHERE Activo = 1
                      AND Procesado = 0
                    ORDER BY IdCliente
                )
                UPDATE CTE
                SET Procesado = 1,
                    FechaProcesado = GETDATE();

                SET @FilasProcesadas = @@ROWCOUNT;

            COMMIT TRAN;

            -- Salida controlada si ya no hay más registros
            IF @FilasProcesadas < @BatchSize
                BREAK;
        END

        INSERT INTO LogProcesos (Mensaje, Fecha)
        VALUES ('Proceso finalizado correctamente', GETDATE());

    END TRY
    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        INSERT INTO LogErrores (Mensaje, Fecha, Procedimiento)
        VALUES (
            ERROR_MESSAGE(),
            GETDATE(),
            'sp_ProcesoClientes_Batch'
        );

        THROW; -- Hace fallar el Job
    END CATCH
END;
GO