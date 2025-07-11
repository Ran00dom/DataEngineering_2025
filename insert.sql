INSERT INTO analytics_data (value_name, value_array)
VALUES (@name, @array)
ON CONFLICT (value_name)
DO UPDATE SET value_array = @array; \* заменяет массив под заданным именем если оно уже существует *\

SELECT * FROM analytics_data;