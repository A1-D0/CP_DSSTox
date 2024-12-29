/*
    Author: Osvaldo Hernandez-Segura
    Description: This script creates the CP_DSSTox DB schemas.
    Note: The indexes and views sections assume the database already exists.
    References: ChatGPT
*/

CREATE TABLE IF NOT EXISTS "DSSTox" (
    "DTXSID" VARCHAR(255) PRIMARY KEY,
    "PREFERRED_NAME" VARCHAR(255) NOT NULL,
    "CASRN" VARCHAR(255) NOT NULL,
    "INCHIKEY" VARCHAR(255),
    "IUPAC_NAME" VARCHAR(255),
    "SMILES" VARCHAR(255),
    "MOLECULAR_FORMULA" VARCHAR(255),
    "AVERAGE_MASS" FLOAT,
    "MONOISOTOPIC_MASS" FLOAT,
    "QSAR_READY_SMILES" VARCHAR(255),
    "MS_READY_SMILES" VARCHAR(255),
    "IDENTIFIER" VARCHAR(255) NOT NULL,
    FOREIGN KEY ("IDENTIFIER") REFERENCES "Identifier"("ID")
);

CREATE TABLE IF NOT EXISTS "QSUR_data" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "DTXSID" VARCHAR(255),
    "preferred_name" VARCHAR(255) NOT NULL,
    "preferred_casrn" VARCHAR(255) NOT NULL,
    "harmonized_function" VARCHAR(255) NOT NULL CHECK ("harmonized_function" IN (
        'additive', 'adhesion_promoter', 'antimicrobial', 'antioxidant', 
        'antistatic_agent', 'buffer', 'catalyst', 'chelator', 'colorant', 
        'crosslinker', 'emollient', 'emulsifier', 'emulsion_stabilizer', 
        'flame_retardant', 'flavorant', 'foam_boosting_agent', 'foamer', 
        'fragrance', 'hair_conditioner', 'hair_dye', 'heat_stabilizer', 
        'humectant', 'lubricating_agent', 'monomer', 'organic_pigment', 
        'oxidizer', 'photoinitiator', 'preservative', 'reducer', 
        'skin_conditioner', 'skin_protectant', 'soluble_dye', 'surfactant', 
        'uv_absorber', 'vinyl', 'wetting_agent', 'whitener'
    )),
    "probability" FLOAT CHECK ("probability" >= 0 AND "probability" <= 1),
    FOREIGN KEY ("DTXSID") REFERENCES "DSSTox"("DTXSID")
);

CREATE TABLE IF NOT EXISTS "Identifier" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "IDENTIFIER" VARCHAR(255),
    "CASRN" VARCHAR(255) NOT NULL,
    "ALTERNATIVE_IDENTIFIER" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS "chemical_dictionary" (
    "chemical_id" INT PRIMARY KEY,
    "raw_chem_name" VARCHAR(255),
    "raw_casrn" VARCHAR(255),
    "preferred_name" VARCHAR(255),
    "preferred_casrn" VARCHAR(255),
    "DTXSID" VARCHAR(255),
    "curation_level" VARCHAR(255) CHECK ("curation_level" IN ('C', 'PR')),
    FOREIGN KEY ("DTXSID") REFERENCES "DSSTox"("DTXSID")
);

CREATE TABLE IF NOT EXISTS "list_presence_dictionary" (
    "list_presence_id" INT PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "definition" VARCHAR(255),
    "kind" VARCHAR(255) NOT NULL CHECK ("kind" IN (
        'General use',
        'PUC - article',
        'PUC - formulation',
        'Location',
        'Manufacturing',
        'Foods & Agriculture',
        'Specialty list',
        'Subpopulation',
        'Media',
        'PUC - industrial',
        'Modifiers'
    ))
);

CREATE TABLE IF NOT EXISTS "list_presence_data" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "document_id" INT NOT NULL,
    "chemical_id" INT,
    "list_presence_id" INT,
    Foreign Key ("document_id") REFERENCES "document_dictionary"("document_id"),
    FOREIGN KEY ("chemical_id") REFERENCES "chemical_dictionary"("chemical_id"),
    FOREIGN KEY ("list_presence_id") REFERENCES "list_presence_dictionary"("list_presence_id")
);

CREATE TABLE IF NOT EXISTS "HHE_data" (
    "document_id" INT NOT NULL,
    "chemical_id" INT,
    PRIMARY KEY ("document_id", "chemical_id"),
    FOREIGN KEY ("document_id") REFERENCES "document_dictionary"("document_id"),
    FOREIGN KEY ("chemical_id") REFERENCES "chemical_dictionary"("chemical_id")
);

CREATE TABLE IF NOT EXISTS "document_dictionary" (
    "document_id" INT PRIMARY KEY,
    "title" VARCHAR(255),
    "subtitle" VARCHAR(255),
    "doc_date" DATE
);

CREATE TABLE IF NOT EXISTS "functional_use_dictionary" (
    "chemical_id" INT NOT NULL,
    "functional_use_id" INT PRIMARY KEY,
    "report_funcuse" VARCHAR(255),
    "oecd_function" VARCHAR(255),
    FOREIGN KEY ("chemical_id") REFERENCES "chemical_dictionary"("chemical_id")
);

CREATE TABLE IF NOT EXISTS "functional_use_data" (
    "document_id" INT NOT NULL,
    "chemical_id" INT NOT NULL,
    "functional_use_id" INT,
    PRIMARY KEY ("document_id", "chemical_id", "functional_use_id"),
    FOREIGN KEY ("document_id") REFERENCES "document_dictionary"("document_id"),
    FOREIGN KEY ("chemical_id") REFERENCES "chemical_dictionary"("chemical_id"),
    FOREIGN KEY ("functional_use_id") REFERENCES "functional_use_dictionary"("functional_use_id")
);

CREATE TABLE IF NOT EXISTS "product_composition_data" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "document_id" INT,
    "product_id" INT,
    "chemical_id" INT,
    "functional_use_id" INT,
    "puc_id" INT NOT NULL,
    "classification" VARCHAR(255) NOT NULL CHECK ("classification" IN ('MA', 'MB', 'PR')),
    "prod_title" VARCHAR(255) NOT NULL,
    "brand_name" VARCHAR(255),
    "raw_min_comp" FLOAT CHECK ("raw_min_comp" >= 0),
    "raw_central_comp" FLOAT CHECK ("raw_central_comp" >= 0),
    "raw_max_comp" FLOAT CHECK ("raw_max_comp" >= 0),
    "clean_min_wf" FLOAT CHECK ("clean_min_wf" >= 0 AND "clean_min_wf" <= 1),
    "clean_central_wf" FLOAT CHECK ("clean_central_wf" >= 0 AND "clean_central_wf" <= 1),
    "clean_max_wf" FLOAT CHECK ("clean_max_wf" >= 0 AND "clean_max_wf" <= 1),
    FOREIGN KEY ("document_id") REFERENCES "document_dictionary"("document_id"),
    FOREIGN KEY ("chemical_id") REFERENCES "chemical_dictionary"("chemical_id"),
    FOREIGN KEY ("functional_use_id") REFERENCES "functional_use_dictionary"("functional_use_id"),
    FOREIGN KEY ("puc_id") REFERENCES "PUC_dictionary"("puc_id")
);

CREATE TABLE IF NOT EXISTS "PUC_dictionary" (
    "puc_id" INT PRIMARY KEY,
    "gen_cat" VARCHAR(255) NOT NULL,
    "prod_fam" VARCHAR(255),
    "prod_type" VARCHAR(255),
    "description" VARCHAR(255) NOT NULL,
    "puc_code" VARCHAR(255),
    "kind" CHAR(1) NOT NULL CHECK ("kind" IN ('F', 'A', 'O'))
);
