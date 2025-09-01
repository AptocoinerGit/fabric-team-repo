[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared RawDataAdventureWorks_Product_Categories = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "0548e0b4-eaba-4f2e-bfe7-dba445249bdb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "389307b6-caa4-4f21-b8cb-3c60df1d0c90"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "RawDataAdventureWorks_Product_Categories"]}[Content],
  Navigation = #"Navigation 4"{[Name = "AdventureWorks_Product_Categories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 2, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductCategoryKey", Int64.Type}, {"CategoryName", type text}}),
  #"Lowercased text" = Table.TransformColumns(#"Changed column type", {{"CategoryName", each Text.Lower(_), type nullable text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Lowercased text", {{"ProductCategoryKey", Int64.Type}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared RawDataProduct_Subcategories = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "0548e0b4-eaba-4f2e-bfe7-dba445249bdb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "389307b6-caa4-4f21-b8cb-3c60df1d0c90"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "RawDataProduct_Subcategories"]}[Content],
  Navigation = #"Navigation 4"{[Name = "AdventureWorks_Product_Subcategories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 3, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductSubcategoryKey", Int64.Type}, {"SubcategoryName", type text}, {"ProductCategoryKey", Int64.Type}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"SubcategoryName", each Text.Upper(_), type nullable text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Uppercased text", {{"ProductCategoryKey", Int64.Type}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared Merge = let
  Source = Table.NestedJoin(RawDataProduct_Subcategories, {"ProductCategoryKey"}, RawDataAdventureWorks_Product_Categories, {"ProductCategoryKey"}, "RawDataAdventureWorks_Product_Categories", JoinKind.LeftOuter),
  #"Expanded RawDataAdventureWorks_Product_Categories" = Table.ExpandTableColumn(Source, "RawDataAdventureWorks_Product_Categories", {"ProductCategoryKey", "CategoryName"}, {"ProductCategoryKey.1", "CategoryName"}),
  #"Inserted conditional column" = Table.AddColumn(#"Expanded RawDataAdventureWorks_Product_Categories", "Customflag", each if [ProductCategoryKey] = 1 then "one" else if [ProductSubcategoryKey] = 2 then "two" else "three")
in
  #"Inserted conditional column";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "0548e0b4-eaba-4f2e-bfe7-dba445249bdb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "389307b6-caa4-4f21-b8cb-3c60df1d0c90"]}[Data]
in
  #"Navigation 2";
