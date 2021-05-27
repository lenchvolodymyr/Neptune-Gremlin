module.exports = ({ _, connection }) => {
	return {
		async getLabels() {
			const response = await connection.submit(`g.V().label().dedup().toList()`);

			return response.toArray();
		},
		async getNodesCount(label) {
			const response = await connection.submit(`g.V().hasLabel('${label}').count().next()`);

			return response.first();
		},

		async getNodes(label, limit = 100) {
			const response = await connection.submit(`g.V().hasLabel('${label}').limit(${limit}).valueMap(true).toList()`);

			return response.toArray().map(getItemProperties(_));
		},

		async getSchema(gremlinElement, label, limit = 100) {
			return { schema: {}, template: []};
		},

		async getRelationshipSchema(labels, limit = 100) {
			return Promise.all(
				labels.map(async label => {
					const relationshipData = await connection.submit(
						`g.V().hasLabel('${label}').outE().limit(${limit}).as('edge').inV().as('end').select('edge', 'end').by(label).dedup().toList()`
					);

					const relationship = _.first(relationshipData.toArray());

					if (!relationship) {
						return {};
					}

					return {
						start: label,
						relationship: relationship.get('edge'),
						end: relationship.get('end')
					};
				}),
			);
		},

		async getCountRelationshipsData(start, relationship, end) {
			const response = await connection.submit(`g.E().hasLabel('${relationship}').where(
				and(
					outV().label().is(eq('${start}')),
					inV().label().is(eq('${end}'))
				)
			).count().next()`);

			return response.toArray();
		},

		async getRelationshipData(start, relationship, end, limit = 100) {
			const response = await connection.submit(`g.E().hasLabel('${relationship}').where(
					and(
						outV().label().is(eq('${start}')),
						inV().label().is(eq('${end}'))
					)
				).limit(${limit}).valueMap(true).toList()`
			);

			return response.toArray().map(getItemProperties(_));
		},
	};
};

const getItemProperties = _ => propertiesMap => {
	return Array.from(propertiesMap).reduce((obj, [key, rawValue]) => {
		if (!_.isString(key)) {
			return obj;
		}

		const value = _.isArray(rawValue) ? _.first(rawValue) : rawValue;

		if (_.isMap(value)) {
			return Object.assign(obj, { [key]: handleMap(value) })
		}

		return Object.assign(obj, { [key]: value });
	}, {});
};
