const config = require('../../config.js');
const getDBClient = require('./dbClient');
const dbClient = getDBClient(config.getConnections().mysql);

var helperRepository = {

    createContentObject: async function (contentObjectType, transaction) {
        const query =
            `INSERT INTO ContentObject (ContentObjectTypeId) VALUES (:contentObjectType);
                       
            SELECT last_insert_id() AS Id;`;

        var connection = await dbClient.getConnectionAsync(transaction);
        var result = await connection.queryAsync(query, {contentObjectType});
        return result[1][0].Id;
	},
    getUserInfo: async function (studentId, transaction) {
        const query =
                `SELECT StudentTypeId, StudentDegreeLevelId, UniversityId, SchoolId, AreaId, CourseId
                        FROM dbo.Student WHERE Id = :studentId`;

        var connection = await dbClient.getConnectionAsync(transaction);
        var result = await connection.queryAsync(query, {studentId});
        return result[0];
    }

};

module.exports = helperRepository;
