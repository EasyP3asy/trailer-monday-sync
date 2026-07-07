// queries/monday.queries.js
// Pure GraphQL query/mutation builders — no network calls, just strings.

function gqlEscape(s = '') {
  return String(s).replace(/\\/g,'\\\\').replace(/"/g,'\\"').replace(/\n/g,'\\n').replace(/\r/g,'\\r');
}

export const deleteGroupQuery = (boardID, groupID) => `
  mutation { delete_group(board_id: ${boardID}, group_id: "${groupID}") { id deleted } }`;

export const duplicateGroupQuery = (boardID, groupID) => `
  mutation { duplicate_group(board_id: ${boardID}, group_id: "${groupID}", add_to_top: true) { id } }`;

export const updateGroupTitleQuery = (boardID, groupID, newTitle) => `
  mutation { update_group(board_id: ${boardID}, group_id: "${groupID}", group_attribute: title, new_value: "${newTitle}") { id } }`;

export const updateGroupColorQuery = (boardID, groupID, color) => `
  mutation { update_group(board_id: ${boardID}, group_id: "${groupID}", group_attribute: color, new_value: "${color}") { id } }`;

export const updateMultipleColumnValuesQuery = (boardID, rowID, columnValues) => `
  mutation { change_multiple_column_values(board_id: ${boardID}, item_id: ${rowID}, column_values: "${gqlEscape(JSON.stringify(columnValues))}") { id } }`;

export const updateMultipleAliasColumnValuesQuery = (boardID, rowID, columnValues) => `
  item_${rowID}: change_multiple_column_values(board_id: ${boardID}, item_id: ${rowID}, column_values: "${gqlEscape(JSON.stringify(columnValues))}") { id }`;

export const createMultipleAliasColumnValuesQuery = (index, boardID, groupId, itemName, columnValues) => `
  item_${index}: create_item(board_id: ${boardID}, group_id: "${groupId}", item_name: "${itemName}", column_values: "${gqlEscape(JSON.stringify(columnValues))}") { id }`;

export const updateSimpleColumnValueQuery = (boardID, rowID, itemName) => `
  mutation { change_simple_column_value(board_id: ${boardID}, item_id: ${rowID}, column_id: "name", value: "${itemName.toUpperCase()}") { id } }`;

export const changeGroupPositionQuery = (boardID, groupId, relativeTo) => `
  mutation { update_group(board_id: ${boardID}, group_id: "${groupId}", group_attribute: relative_position_before, new_value: "${relativeTo}") { id } }`;

export const getGroupsIDandTitleQuery = (boardID) => `
  query { boards(ids: ${boardID}) { groups { title id } } }`;

export const getColumnValues = (boardID) => `
  query { boards(ids: ${boardID}) { items_page { items { id name group { id } column_values { id text value } } } } }`;

export const getAllRowIDs = (boardID) => `
  query { boards(ids: ${boardID}) { items_page(limit: 499) { cursor items { id name } } } }`;

export const getConnectedBoardId_ItemId = (itemId, colId) => `
  query { items(ids: [${itemId}]) { column_values(ids: ["${colId}"]) { ... on BoardRelationValue { linked_items { id name board { id } } } } } }`;