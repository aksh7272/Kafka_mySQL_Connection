CREATE TABLE product (
    id int IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
	category VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    last_updated Datetime2
);

select * from product;

INSERT INTO product (name, category, price, last_updated)
VALUES
('Desktop', 'Electronics', 1200.00, '2025-02-01 09:00:00'),
('phone', 'Electronics', 850.00, '2025-02-01 09:05:00')


UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 1;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 2;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 3;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 4;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 5;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 6;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 7;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 8;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 9;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 10;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 11;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 12;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 13;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 14;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 15;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 16;
UPDATE product SET price = price + 10, last_updated = GETDATE() WHERE id = 17;



