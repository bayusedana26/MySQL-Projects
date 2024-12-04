SELECT * FROM students_performance sp 

-- sp get all data
CREATE PROCEDURE allSpData()
BEGIN
	SELECT * FROM students_performance;
END

CALL allSpData()

-- sp input for ras
CREATE PROCEDURE getRace(IN race VARCHAR(20))
BEGIN
	SELECT * FROM students_performance WHERE race_or_ethnicity = race;
END

CALL getRace('group B') 

-- sp avg math score
CREATE PROCEDURE avgMath(OUT avg_math INT)
BEGIN
	SELECT AVG(math_score) INTO avg_math FROM students_performance;
END

CALL avgMath(@avg_math)

SELECT @avg_math

-- sp using out exceeded avg math score
CREATE PROCEDURE passAvgMath()
BEGIN
	SELECT * FROM students_performance WHERE math_score >= 66;
END

CALL passAvgMath() 

-- sp reading score > avg math score
SELECT * FROM students_performance sp WHERE reading_score > @avg_math

-- sp for male or female avg math score
CREATE PROCEDURE genderAvgMath(IN sex VARCHAR(10), OUT avg_math_score INT)
BEGIN 
	SELECT
	AVG(math_score) INTO avg_math_score
	FROM
	students_performance
	WHERE
	gender = sex;
END

SET @sex = 'male'

CALL genderAvgMath(@sex, @avg_math_score)

SELECT @sex, @avg_math_score

-- fill id_buku column
CREATE TABLE books (
book_id INT
)

SELECT * FROM books 

CREATE PROCEDURE fill_book_id()
BEGIN
	DECLARE counter INT;
	SET counter = 1;
	
	WHILE counter <= 10 DO
	INSERT INTO books (book_id) VALUES (counter);
	SET counter = counter + 1;
	END WHILE;
END

CALL fill_book_id()

SELECT * FROM books 

-- sp untuk hitung luas bangunan
CREATE PROCEDURE calculate_shape(
	IN shape_type VARCHAR(20), 
	IN lenght FLOAT, 
	IN width FLOAT, 
	OUT breadth FLOAT,
	OUT description VARCHAR(50))
BEGIN
	CASE
		WHEN shape_type = 'Triangle' THEN SET breadth = 0.5 * lenght * width, description = 'Success!';
		WHEN shape_type = 'Rectangle' THEN SET breadth = lenght * width, description = 'Success!';
		ELSE SET breadth = NULL, description = 'Not 2d figure, failed to calculate';
	END CASE;
END

-- Triangle shape
SET @shape = 'Triangle'

SET @panjang = 15.5

SET @lebar = 5.5

CALL calculate_shape(@shape, @panjang, @lebar, @breadth, @description)

SELECT @shape, @panjang, @lebar, @breadth, @description

-- Rectangle shape
SET @shape = 'Rectangle'

SET @panjang = 10

SET @lebar = 15

CALL calculate_shape(@shape, @panjang, @lebar, @breadth, @description)

SELECT @shape, @panjang, @lebar, @breadth, @description

-- Non 2d figure shape
SET @shape = 'Round'

SET @panjang = 7.5

SET @lebar = 9.5

CALL calculate_shape(@shape, @panjang, @lebar, @breadth, @description)

SELECT @shape, @panjang, @lebar, @breadth, @description

-- sp status
SHOW PROCEDURE STATUS

DROP PROCEDURE IF EXISTS getRace

DROP PROCEDURE IF EXISTS avgMath

DROP PROCEDURE IF EXISTS passAvgMath

DROP PROCEDURE IF EXISTS genderAvgMAth

DROP PROCEDURE IF EXISTS calculate_shape

