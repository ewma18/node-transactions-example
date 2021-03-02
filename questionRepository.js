const config = require('../../config.js');
const dbClient = require('pd-node-mysql-client').getClient(config.getConnections().mysql);


const questionRepository = {
	getMaterial: async function (materialId, transaction) {
		const query =
			`SELECT DISTINCT MU.Id, MU.SubjectId, MU.UserId, MU.Title, MU.FilledDate, 
																MU.MaterialLink, MU.Tags, MU.MaterialListId 
										FROM dbo.MaterialUpload MU
								WHERE MU.Id = :materialId;`;

		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, {
			materialId
		});
		return result[0];
	},
	getQuestion: async function (questionId) {
		const query =
			`SELECT Id, Text, Closed, AnswersCount, ModerationType, PremiumAnswerId, Archived 
			FROM Question 
			WHERE Id = :questionId;`;

		var result = await dbClient.queryAsync(query, {
			questionId
		});

		return result[0];
	},
	getLastQuestion: async function (studentId) {
		const query =
			`SELECT M.Name as Title, Text from Question Q 
			  INNER JOIN Material M ON M.Id = Q.Id
				WHERE M.UserId = :studentId
				ORDER BY M.CreationDate DESC
				LIMIT 1;`;

		var result = await dbClient.queryAsync(query, {
			studentId
		});

		return result[0];
	},
	createQuestion: async function (material, transaction) {

		const query =
			`INSERT INTO dbo.Question
			(Id, Text, Closed, AnswersCount, ModerationType, PremiumAnswerId, Archived) 
					VALUES (:Id, :Text, :Closed, :AnswersCount, :ModerationType, :PremiumAnswerId, Archived);`;

		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, material);
		return result[0];
	},
	updateQuestion: async function (question, transaction) {

		const query =
			`UPDATE dbo.Question
			 SET 
				Text = :Text, 
				Closed = :Closed, 
				AnswersCount = :AnswersCount, 
				ModerationType = :ModerationType, 
				PremiumAnswerId = :PremiumAnswerId, 
				Archived = :Archived
			WHERE Id = :Id;`;

		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, question);
		return result[0];
	}
}


module.exports =
	questionRepository;