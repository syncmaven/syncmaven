export type GenericColumnType = "integer" | "string" | "boolean" | "date" | "float";

export type ColumnType = {
  //native DB type
  nativeType: string;
  //common type for the platform
  genericType: GenericColumnType;
};
