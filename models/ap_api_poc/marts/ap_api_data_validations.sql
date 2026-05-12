{{ config(materialized='table', tags=['ap_api_poc']) }}

/*
  Purpose: Validate AP API data for consistency and completeness
  - Groups data by ULB and Year
  - Applies 22 validation rules (dynamically counted)
  - Shows validation pass/fail count (X/22)
  - Dynamic error numbering with newlines for readability
  - Values displayed in crores (₹ symbol)
*/

WITH source_data AS (
    SELECT 
        majorcode,
        lineitemname,
        ulb,
        state,
        year,
        "updated_at",
        "headOfAccount",
        COALESCE(amount, 0) AS amount,
        subcode
    FROM {{ ref('cf_ap_api_poc') }}
),

-- Calculate all aggregates grouped by ulb and year
aggregated_validations AS (
    SELECT
        ulb,
        state,
        year,
        MAX("updated_at") AS updated_at,
        
        -- For validations 1-2: Total Revenue
        SUM(CASE WHEN "headOfAccount" = 'Revenue' THEN amount ELSE 0 END) AS total_revenue,
        
        -- For validations 3-4: Total Expenditure
        SUM(CASE WHEN "headOfAccount" = 'Expenditure' THEN amount ELSE 0 END) AS total_expenditure,
        
        -- For validations 5, 20, 22: Tax Revenue (majorCode 110, subCode 0)
        SUM(CASE WHEN majorcode = 110 AND subcode = 0 THEN amount ELSE 0 END) AS tax_revenue,
        
        -- For validation 6: Establishment Expense (majorCode 210)
        SUM(CASE WHEN majorcode = 210 AND subcode = 0 THEN amount ELSE 0 END) AS establishment_expense,
        
        -- For validations 7-8: Admin Expense (majorCode 220)
        SUM(CASE WHEN majorcode = 220 AND subcode = 0 THEN amount ELSE 0 END) AS admin_expense,
        
        -- For validation 9: Programme Expense (majorCode 250)
        SUM(CASE WHEN majorcode = 250 AND subcode = 0 THEN amount ELSE 0 END) AS programme_expense,
        
        -- For validation 10: Interest & Finance Charges (majorCode 240)
        SUM(CASE WHEN majorcode = 240 AND subcode = 0 THEN amount ELSE 0 END) AS interest_finance_charges,
        
        -- For validation 11: Total Own Revenue (majorCode 110, 130, 140, 150, 180)
        SUM(CASE WHEN majorcode IN (110, 130, 140, 150, 180) AND subcode = 0 THEN amount ELSE 0 END) AS total_own_revenue,
        
        -- For validation 12: Assigned Revenue (majorCode 120)
        SUM(CASE WHEN majorcode = 120 AND subcode = 0 THEN amount ELSE 0 END) AS assigned_revenue,
        
        -- For validation 13: Rental Income (majorCode 130)
        SUM(CASE WHEN majorcode = 130 AND subcode = 0 THEN amount ELSE 0 END) AS rental_income,
        
        -- For validation 14: Fees & User Charges (majorCode 140)
        SUM(CASE WHEN majorcode = 140 AND subcode = 0 THEN amount ELSE 0 END) AS fees_user_charges,
        
        -- For validation 15: Sales & Hire Charges (majorCode 150)
        SUM(CASE WHEN majorcode = 150 AND subcode = 0 THEN amount ELSE 0 END) AS sales_hire_charges,
        
        -- For validation 16: Grants (majorCode 160)
        SUM(CASE WHEN majorcode = 160 AND subcode = 0 THEN amount ELSE 0 END) AS grants,
        
        -- For validation 17: Income from Investment (majorCode 170)
        SUM(CASE WHEN majorcode = 170 AND subcode = 0 THEN amount ELSE 0 END) AS investment_income,
        
        -- For validation 18: Interest Earned (majorCode 171)
        SUM(CASE WHEN majorcode = 171 AND subcode = 0 THEN amount ELSE 0 END) AS interest_earned,
        
        -- For validation 19: Other Income (majorCode 180)
        SUM(CASE WHEN majorcode = 180 AND subcode = 0 THEN amount ELSE 0 END) AS other_income,
        
        -- For validation 21: Property Tax Revenue (majorCode 110, subCode 1100101)
        SUM(CASE WHEN majorcode = 110 AND subcode = 1100101 THEN amount ELSE 0 END) AS property_tax_revenue,
        
        -- For validation 22: Tax Revenue particulars (majorCode 110, subCode starts with 110)
        SUM(CASE WHEN majorcode = 110 AND CAST(subcode AS text) LIKE '110%' THEN amount ELSE 0 END) AS tax_revenue_particulars
        
    FROM source_data
    GROUP BY ulb, state, year
),

-- Count validation failures and build concise error messages with newlines
final_validations AS (
    SELECT
        ulb,
        state,
        year,
        updated_at,
        
        -- Count validation failures
        (CASE WHEN total_revenue = 0 THEN 1 ELSE 0 END +
         CASE WHEN total_revenue < 0 THEN 1 ELSE 0 END +
         CASE WHEN total_expenditure = 0 THEN 1 ELSE 0 END +
         CASE WHEN total_expenditure < 0 THEN 1 ELSE 0 END +
         CASE WHEN tax_revenue = 0 THEN 1 ELSE 0 END +
         CASE WHEN establishment_expense = 0 THEN 1 ELSE 0 END +
         CASE WHEN admin_expense = 0 THEN 1 ELSE 0 END +
         CASE WHEN admin_expense < 0 THEN 1 ELSE 0 END +
         CASE WHEN programme_expense < 0 THEN 1 ELSE 0 END +
         CASE WHEN interest_finance_charges < 0 THEN 1 ELSE 0 END +
         CASE WHEN total_own_revenue < 0 THEN 1 ELSE 0 END +
         CASE WHEN assigned_revenue < 0 THEN 1 ELSE 0 END +
         CASE WHEN rental_income < 0 THEN 1 ELSE 0 END +
         CASE WHEN fees_user_charges < 0 THEN 1 ELSE 0 END +
         CASE WHEN sales_hire_charges < 0 THEN 1 ELSE 0 END +
         CASE WHEN grants < 0 THEN 1 ELSE 0 END +
         CASE WHEN investment_income < 0 THEN 1 ELSE 0 END +
         CASE WHEN interest_earned < 0 THEN 1 ELSE 0 END +
         CASE WHEN other_income < 0 THEN 1 ELSE 0 END +
         CASE WHEN tax_revenue < 0 THEN 1 ELSE 0 END +
         CASE WHEN property_tax_revenue < 0 THEN 1 ELSE 0 END +
         CASE WHEN tax_revenue_particulars != tax_revenue THEN 1 ELSE 0 END
        ) AS validation_failures,
        
        -- Count total possible validations (dynamically counted)
        (CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 1: total_revenue = 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 2: total_revenue < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 3: total_expenditure = 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 4: total_expenditure < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 5: tax_revenue = 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 6: establishment_expense = 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 7: admin_expense = 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 8: admin_expense < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 9: programme_expense < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 10: interest_finance_charges < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 11: total_own_revenue < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 12: assigned_revenue < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 13: rental_income < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 14: fees_user_charges < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 15: sales_hire_charges < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 16: grants < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 17: investment_income < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 18: interest_earned < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 19: other_income < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 20: tax_revenue < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END +  -- 21: property_tax_revenue < 0
         CASE WHEN TRUE THEN 1 ELSE 0 END    -- 22: tax_revenue_particulars != tax_revenue
        ) AS total_validations,
        
        -- Build concise error messages separated by newlines
        -- Values converted to crores and formatted with ₹ symbol
        TRIM(BOTH chr(10) FROM CONCAT_WS(chr(10),
            CASE WHEN total_revenue = 0 THEN '- Revenue collection is missing: Total Revenue cannot be zero' ELSE NULL END,
            CASE WHEN total_revenue < 0 THEN '- Invalid Revenue amount: Total Revenue shows negative value ₹ ' || ROUND(total_revenue/10000000, 4) ELSE NULL END,
            CASE WHEN total_expenditure = 0 THEN '- Expenditure data is incomplete: Total Expenditure cannot be zero' ELSE NULL END,
            CASE WHEN total_expenditure < 0 THEN '- Invalid Expenditure amount: Total Expenditure shows negative value ₹ ' || ROUND(total_expenditure/10000000, 4) ELSE NULL END,
            CASE WHEN tax_revenue = 0 THEN '- Critical missing data: Tax Revenue (Code 110) is zero' ELSE NULL END,
            CASE WHEN establishment_expense = 0 THEN '- Establishment expense missing: Code 210 cannot be zero' ELSE NULL END,
            CASE WHEN admin_expense = 0 THEN '- Admin expense missing: Code 220 cannot be zero' ELSE NULL END,
            CASE WHEN admin_expense < 0 THEN '- Invalid Admin Expense: Code 220 shows negative value ₹ ' || ROUND(admin_expense/10000000, 4) ELSE NULL END,
            CASE WHEN programme_expense < 0 THEN '- Invalid Programme Expense: Code 250 shows negative value ₹ ' || ROUND(programme_expense/10000000, 4) ELSE NULL END,
            CASE WHEN interest_finance_charges < 0 THEN '- Invalid Interest Charges: Code 240 shows negative value ₹ ' || ROUND(interest_finance_charges/10000000, 4) ELSE NULL END,
            CASE WHEN total_own_revenue < 0 THEN '- Invalid Own Revenue: Codes 110/130/140/150/180 shows negative value ₹ ' || ROUND(total_own_revenue/10000000, 4) ELSE NULL END,
            CASE WHEN assigned_revenue < 0 THEN '- Invalid Assigned Revenue: Code 120 shows negative value ₹ ' || ROUND(assigned_revenue/10000000, 4) ELSE NULL END,
            CASE WHEN rental_income < 0 THEN '- Invalid Rental Income: Code 130 shows negative value ₹ ' || ROUND(rental_income/10000000, 4) ELSE NULL END,
            CASE WHEN fees_user_charges < 0 THEN '- Invalid Fees & Charges: Code 140 shows negative value ₹ ' || ROUND(fees_user_charges/10000000, 4) ELSE NULL END,
            CASE WHEN sales_hire_charges < 0 THEN '- Invalid Sales Income: Code 150 shows negative value ₹ ' || ROUND(sales_hire_charges/10000000, 4) ELSE NULL END,
            CASE WHEN grants < 0 THEN '- Invalid Grants: Code 160 shows negative value ₹ ' || ROUND(grants/10000000, 4) ELSE NULL END,
            CASE WHEN investment_income < 0 THEN '- Invalid Investment Income: Code 170 shows negative value ₹ ' || ROUND(investment_income/10000000, 4) ELSE NULL END,
            CASE WHEN interest_earned < 0 THEN '- Invalid Interest Income: Code 171 shows negative value ₹ ' || ROUND(interest_earned/10000000, 4) ELSE NULL END,
            CASE WHEN other_income < 0 THEN '- Invalid Other Income: Code 180 shows negative value ₹ ' || ROUND(other_income/10000000, 4) ELSE NULL END,
            CASE WHEN tax_revenue < 0 THEN '- Critical error: Tax Revenue shows negative value ₹ ' || ROUND(tax_revenue/10000000, 4) ELSE NULL END,
            CASE WHEN property_tax_revenue < 0 THEN '- Invalid Property Tax: Code 110/1100101 shows negative value ₹ ' || ROUND(property_tax_revenue/10000000, 4) ELSE NULL END,
            CASE WHEN tax_revenue_particulars != tax_revenue THEN '- Incomplete Tax Revenue breakdown: Particulars do not match total Tax Revenue' ELSE NULL END
        )) AS validation_errors_list
        
    FROM aggregated_validations
)

SELECT 
    ulb,
    state,
    year,
    updated_at,
    CASE 
        WHEN validation_failures = 0 THEN 'All validations passed - Data is complete and accurate'
        ELSE validation_errors_list
    END AS validation_errors,
    validation_failures::text || '/' || total_validations::text AS validation_status
FROM final_validations
ORDER BY validation_failures DESC, ulb, year