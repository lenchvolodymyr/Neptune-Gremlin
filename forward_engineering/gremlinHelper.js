const DEFAULT_INDENT = '    ';
let graphName = 'g';

module.exports = (_) => {
	const generateVertex = (collection, vertexData) => {
		const vertexName = transformToValidGremlinName(collection.collectionName);
		const propertiesScript = addPropertiesScript(collection, vertexData);

		return `${graphName}.addV(${JSON.stringify(vertexName)})${propertiesScript}`;
	};

	const generateVertices = (collections, jsonData) => {
		const vertices = collections.map(collection => {
			const vertexData = JSON.parse(jsonData[collection.GUID]);

			return generateVertex(collection, vertexData)
		});	

		const script = vertices.join(';\n\n');
		if (!script) {
			return '';
		}

		return script + ';';
	}

	const generateEdge = (from, to, relationship, edgeData) => {
		const edgeName = transformToValidGremlinName(relationship.name);
		const propertiesScript = addPropertiesScript(relationship, edgeData, { cardinality: false });

		return `${graphName}.addE(${JSON.stringify(edgeName)}).\n${DEFAULT_INDENT}from(${from}).\n${DEFAULT_INDENT}to(${to})${propertiesScript}`;
	};

	const getVertexVariableScript = vertexName => `${graphName}.V().hasLabel(${JSON.stringify(vertexName)})`;

	const generateEdges = (collections, relationships, jsonData) => {
		const edges = relationships.reduce((edges, relationship) => {
			const parentCollection = collections.find(collection => collection.GUID === relationship.parentCollection);
			const childCollection = collections.find(collection => collection.GUID === relationship.childCollection);
			if (!parentCollection || !childCollection) {
				return edges;
			}
			const from = transformToValidGremlinName(parentCollection.collectionName);
			const to = transformToValidGremlinName(childCollection.collectionName);
			const edgeData = JSON.parse(jsonData[relationship.GUID]);

			return edges.concat(generateEdge(getVertexVariableScript(from), getVertexVariableScript(to), relationship, edgeData));
		}, []);

		if (_.isEmpty(edges)) {
			return '';
		}

		return edges.join(';\n\n') + ';';
	}

	const addPropertiesScript = (collection, vertexData, features = { cardinality: true }) => {
		const properties = _.get(collection, 'properties', {});

		return Object.keys(properties).reduce((script, name) => {
			const property = properties[name];

			return script + createProperty(property, name, vertexData[name], features);
		}, '');
	};

	const createProperty = (property, name, vertexData, features) => {
		const type = property.type;

		if (type === 'multi-property') {
			return convertSet(property, name, vertexData, features);
		}

		const valueScript = convertPropertyValue(property, 2, type, vertexData);
		const cardinality = features.cardinality ? `${property.propCardinality || 'single'}, ` : '';

		return `.\n${DEFAULT_INDENT}property(${cardinality}${JSON.stringify(name)}, ${valueScript})`
	};

	const convertSet = (property, name, vertexData, features) => {
		const items = Array.isArray(property.items) ? property.items : [property.items];

		return items.map((item, i) => createProperty(
			{...item, propCardinality: 'set'},
			name,
			Array.isArray(vertexData) ? vertexData[i] || '' : '',
			features,
		)).join('');
	};

	const convertDate = value => `datetime("${value}")`;

	const convertNumber = (property, value) => {
		const mode = property.mode;
		const numberValue = JSON.stringify(value);

		switch(mode) {
			case 'double':
				return `${numberValue}d`;
			case 'float':
				return `${numberValue}f`;
			case 'long':
				return `${numberValue}l`;
			default:
				return numberValue;
		}
	};

	const convertPropertyValue = (property, level, type, value) => {
		switch(type) {
			case 'date':
				return convertDate(value);
			case 'number':
				return convertNumber(property, value);
		}

		return `${JSON.stringify(value === undefined ? '' : value)}`;
	};

	const transformToValidGremlinName = (name) => {
		const DEFAULT_NAME = 'New_vertex';
		const DEFAULT_PREFIX = 'v_';

		if (!name || !_.isString(name)) {
			return DEFAULT_NAME;
		}

		const nameWithoutSpecialCharacters = name.replace(/[\s`~!@#%^&*()_|+\-=?;:'",.<>\{\}\[\]\\\/]/gi, '_');
		const startsFromDigit = nameWithoutSpecialCharacters.match(/^[0-9].*$/);

		if (startsFromDigit) {
			return `${DEFAULT_PREFIX}_${nameWithoutSpecialCharacters}`;
		}

		return nameWithoutSpecialCharacters;
	};

	
	return {
		transformToValidGremlinName,
		generateVertices,
		generateEdges,
	};
};