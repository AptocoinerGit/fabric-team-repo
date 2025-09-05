-- Auto Generated (Do not modify) 601170E5BB8A35426FD7EACBE0C066A8D7447442CC3693513C148F9B56AE9873
CREATE VIEW [Gold].[Customer_view] AS (select [_].[CustomerKey] as [CustomerKey],
    [_].[Prefix] as [Prefix],
    [_].[FirstName] as [FirstName],
    [_].[LastName] as [LastName],
    [_].[BirthDate] as [BirthDate],
    upper([_].[MaritalStatus]) as [MaritalStatus],
    [_].[Gender] as [Gender],
    [_].[EmailAddress] as [EmailAddress],
    [_].[AnnualIncome] as [AnnualIncome],
    [_].[TotalChildren] as [TotalChildren],
    [_].[EducationLevel] as [EducationLevel],
    [_].[Occupation] as [Occupation],
    [_].[HomeOwner] as [HomeOwner],
    [_].[FullName] as [FullName],
    [_].[Domain] as [Domain]
from [VamshiWarehouse].[Gold].[dim_customers] as [_])