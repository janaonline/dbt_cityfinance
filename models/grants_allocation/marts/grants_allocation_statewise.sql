{{ config(
    materialized='table',
    tags=['grants_allocation', 'marts']
) }}

SELECT
    "Year",
    "State_Name",
    "Total_Rec_Amount",
    "Total_Rel_Amount",
    cast("Total_Alloc_Amount" as numeric(18,2)) as total_alloc_amount,
    "Million_Plus_with_Rec_Amount",
    "Million_Plus_with_Rel_Amount",
    "Million_Plus_with_Alloc_Amount",
    "Million_Plus_with_Alloc_Amount_1",
    "Million_Plus_with_Date_of_Release",
    "Million_Plus_with_Date_of_Rec_to_DoE",
    "First_Instl_of_NMPC_Tied_with_Rec_Amount",
    "First_Instl_of_NMPC_Tied_with_Rel_Amount",
    "Second_Instl_of_NMPC_Tied_with_Rec_Amount",
    "Second_Instl_of_NMPC_Tied_with_Rel_Amount",
    "First_Instl_of_NMPC_Tied_with_Alloc_Amount",
    "First_Instl_of_NMPC_Un_Tied_with_Rec_Amount",
    "First_Instl_of_NMPC_Un_Tied_with_Rel_Amount",
    "Second_Instl_of_NMPC_Tied_with_Alloc_Amount",
    "Second_Instl_of_NMPC_Un_Tied_with_Rec_Amount",
    "Second_Instl_of_NMPC_Un_Tied_with_Rel_Amount",
    "First_Instl_of_NMPC_Tied_with_Date_of_Release",
    "First_Instl_of_NMPC_Un_Tied_with_Alloc_Amount",
    "Second_Instl_of_NMPC_Tied_with_Date_of_Release",
    "Second_Instl_of_NMPC_Un_Tied_with_Alloc_Amount",
    "First_Instl_of_NMPC_Tied_with_Date_of_Rec_to_DoE",
    "First_Instl_of_NMPC_Un_Tied_with_Date_of_Release",
    "Second_Instl_of_NMPC_Tied_with_Date_of_Rec_to_DoE",
    "Second_Instl_of_NMPC_Un_Tied_with_Date_of_Release",
    "First_Instl_of_NMPC_Tied_with_Date_of_Rec_to_MoHUA",
    "First_Instl_of_NMPC_Un_Tied_with_Date_of_Rec_to_DoE",
    "Second_Instl_of_NMPC_Tied_with_Date_of_Rec_to_MoHUA",
    "Second_Instl_of_NMPC_Un_Tied_with_Date_of_Rec_to_DoE",
    "First_Instl_of_NMPC_Un_Tied_with_Date_of_Rec_to_MoHUA",
    "Second_Instl_of_NMPC_Un_Tied_with_Date_of_Rec_to_MoHUA"
FROM {{ source('cf_grants_allocation', 'grants_allocation_statewise') }}

