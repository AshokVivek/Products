CREATE TABLE metadata_categories(
    id SERIAL PRIMARY KEY,
    category    VARCHAR(255) not null,
    category_description    TEXT,
    added_by    VARCHAR(255),
    is_active   BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('creditor_name', 'Name of the individual', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('creditor_ifsc', 'IFSC of the creditor', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('creditor_upi_handle', 'UPI Handle of the creditor', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('creditor_bank', 'Creditor Bank', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('creditor_account_number', 'Creditor Account Number', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('receiver_name', 'Name of the reciever', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('receiver_ifsc', 'IFSC of the receiver', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('receiver_upi_handle', 'UPI Handle of the receiver', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('receiver_bank', 'Receiver Bank', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('reciever_account_number', 'Receiver Account Number', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('merchant_name', 'Merchant Name', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('merchant_ifsc', 'Merchant IFSC', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('merchant_upi_handle', 'Merchant UPI Handle', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('merchant_bank', 'Merchant Bank Name', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('cheque_number', 'CHQ Number of the related transaction', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('transaction_reference_1', 'Reference Numbers related to the transaction', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('transaction_reference_2', 'Reference Numbers related to the transaction', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('primary_channel', 'Transaction Channel Data', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('secondary_channel', 'Transaction Channel Data', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('tertiary_channel', 'Transaction Channel Data', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('raw_location', 'Location Data present in transaction note', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('transaction_timestamp', 'Transaction Timestamp in transaction Note', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('transaction_amount', 'Transaction Amount in transaction Note', 'admin');
INSERT INTO metadata_categories (category, category_description, added_by) VALUES ('currency', 'Currency present in transaction note', 'admin');