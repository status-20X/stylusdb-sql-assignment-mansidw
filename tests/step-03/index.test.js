// tests/index.test.js

const { parseSelectQuery } = require("../../src/queryParser");

test("Parse SQL Query", () => {
  const query = "SELECT id, name FROM sample";
  const parsed = parseSelectQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "sample",
    whereClauses: [],
    joinTable: null,
    joinCondition: null,
    joinType: null,
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    isDistinct: false,
    limit: null,
    orderByFields: null,
  });
});
