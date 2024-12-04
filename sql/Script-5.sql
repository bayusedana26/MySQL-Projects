DROP PROCEDURE IF EXISTS open_data_jabar.`getAllData()`;

DELIMITER //
CREATE PROCEDURE getAllData()
BEGIN
    SELECT * FROM umkm_jabar;
END //
DELIMITER ;

