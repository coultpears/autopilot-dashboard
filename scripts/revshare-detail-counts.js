// Count units and stays on one rev-share statement tab.
//
// Every statement tab (Jun 2025 onward) carries two detail blocks under the financial summary:
//
//   Unit Level Detail
//   Unit Number | Occupied Nights | Rent | ...        <- one row per installed unit
//   Reservation Level Detail
//   Unit Number | Reference ID | Start Date | ...     <- one row per reservation in the month
//
// The ingest used to count every column-A cell that looked like an id (/^\d/ or /^[A-Za-z]\d/)
// anywhere on the tab and store it as "stays". That summed BOTH blocks: Flagler Crossing
// (48231) read 25 for August 2026 = 15 unit rows + 10 reservation rows, and 11 for July
// (11 units, no reservations yet). It also missed any unit number that doesn't start with a
// digit ("PH1", "Unit B") and would count stray id-like cells outside the blocks.
//
// Now each block is counted on its own, by position, not by what the unit number looks like:
//   units = rows in Unit Level Detail
//   stays = rows in Reservation Level Detail
// A row counts when column A or B has content; the column-header row and any "Total" row
// are skipped. A block that isn't on the tab returns null (unknown), not 0.

const UNITS_HEAD = /^unit\s+level\s+detail/i;
const RES_HEAD = /^reservation\s+level\s+detail/i;
const COL_HEAD = /^(unit\s*number|unit|home\s*id)$/i;

function cell(row, i) {
  return String(row && row[i] != null ? row[i] : '').trim();
}

function countDetail(values) {
  let section = null;
  let units = 0, stays = 0, sawUnits = false, sawRes = false;
  for (const row of values || []) {
    const a = cell(row, 0), b = cell(row, 1);
    if (UNITS_HEAD.test(a)) { section = 'units'; sawUnits = true; continue; }
    if (RES_HEAD.test(a)) { section = 'res'; sawRes = true; continue; }
    if (!section) continue;
    if (!a && !b) continue;
    if (COL_HEAD.test(a)) continue;
    if (/^total\b/i.test(a)) continue;
    if (section === 'units') units++; else stays++;
  }
  return { units: sawUnits ? units : null, stays: sawRes ? stays : null };
}

module.exports = { countDetail };
