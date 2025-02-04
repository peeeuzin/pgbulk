import { Transform, TransformCallback } from "stream";
import { PopulateConfig } from "./populate";

export class ColumnParserTransformer extends Transform {
  tables: PopulateConfig["tables"];

  constructor(tables: PopulateConfig["tables"]) {
    super({ objectMode: true });
    this.tables = tables;
  }

  async _transform(
    row: { [column: string]: unknown },
    enc: BufferEncoding,
    callback: TransformCallback
  ) {
    const transformedRow: { [column: string]: unknown } = {};

    for (const [key, value] of Object.entries(row)) {
      for (const [tableName, columns] of Object.entries(this.tables)) {
        const column = columns.find(
          (col) =>
            col.csvColumn === key ||
            (col.csvColumn === undefined && col.databaseColumn === key)
        );

        if (column) {
          transformedRow[`${tableName}_${column.databaseColumn}`] = value;
          break;
        }
      }
    }

    const allColumns = Object.entries(this.tables).flatMap(
      ([tableName, columns]) => columns.map((c) => ({ ...c, tableName }))
    );

    const refColumns = allColumns.filter(({ ref }) => ref);

    for (const column of refColumns) {
      const referencedColumn = allColumns.find((col) => {
        return (
          col.csvColumn === column.ref ||
          (col.csvColumn === undefined && col.databaseColumn === column.ref)
        );
      });

      if (!referencedColumn) return;

      transformedRow[`${column.tableName}_${column.databaseColumn}`] =
        row[referencedColumn.csvColumn || referencedColumn.databaseColumn];
    }

    callback(null, transformedRow);
  }
}
