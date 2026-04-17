async function runWithConcurrencyLimit(tasks, limit) {
  const results = [];
  const executing = new Set();

  for (const task of tasks) {
    const p = Promise.resolve().then(() => task());
    results.push(p);

    const e = p
      .then(() => executing.delete(e))
      .catch(() => executing.delete(e));
    executing.add(e);

    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.allSettled(results);
}

function jsonToCSVRow(obj, headers) {
  if (!headers) throw new Error(`Headers not initialized for CSV generation`);
  return headers
    .map((header) => {
      const value = obj[header] ?? "";
      if (
        typeof value === "string" &&
        (value.includes(",") || value.includes('"') || value.includes("\n"))
      ) {
        return `"${value.replace(/"/g, '""')}"`;
      }
      return value;
    })
    .join(",");
}

module.exports = {
  runWithConcurrencyLimit,
  jsonToCSVRow,
};
