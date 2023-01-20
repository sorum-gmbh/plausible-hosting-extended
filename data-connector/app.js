const { createClient } = require('@clickhouse/client')
const express = require('express')

const app = express()
const port = 3123

app.use(express.json())

const client = createClient({
	host: 'http://127.0.0.1:8123',
	username: 'default',
	password: ''
})

const DIMENSIONS = ['visitors', 'events', 'pageviews']

app.post('/stats', async (req, res) => {
	const fields = req.body.fields ?? []
	const domain = req.body.domain
	const startDate = req.body.startDate
	const endDate = req.body.endDate

	const query_fields = fields
		.map(field => {
			switch (field) {
				case 'visitors':
					return 'COUNT(DISTINCT user_id) AS visitors'
				case 'events':
					return 'COUNT() as events'
				case 'pageviews':
					return "COUNT() filter (where plausible_events_db.events.name = 'pageview') as pageviews"
				case 'event_name':
					return 'name as event_name'
				case 'entry_page':
					return 'session_entries.entry_page'
				case 'date':
					return 'toString(toYYYYMMDD(timestamp)) as date'
				default:
					return field
			}
		})
		.join(', ')

	const group_by_fields = fields
		.filter(field => !DIMENSIONS.includes(field))
		.map(field => {
			switch (field) {
				case 'event_name':
					return 'name'
				case 'date':
					return 'toString(toYYYYMMDD(timestamp))'
				case 'entry_page':
					return 'session_entries.entry_page'
				default:
					return field
			}
		})
		.join(', ')

	const where = `WHERE domain='${domain}' AND timestamp between '${startDate} 00:00:00' and '${endDate} 23:59:59'`

	const query = `\
		${
			fields.includes('entry_page')
				? `\
				WITH session_entries as (
					WITH session_start AS (
						SELECT session_id, MIN(timestamp) as session_start
						FROM plausible_events_db.events
						${where}
						GROUP BY session_id
					)
					SELECT session_id, session_start, plausible_events_db.events.pathname as entry_page
					FROM session_start
					JOIN plausible_events_db.events
					ON session_start.session_id = plausible_events_db.events.session_id AND session_start.session_start = plausible_events_db.events.timestamp
					${where}
				)`
				: ''
		}
		SELECT ${query_fields}
		FROM plausible_events_db.events
		${
			fields.includes('entry_page')
				? 'JOIN session_entries ON plausible_events_db.events.session_id = session_entries.session_id'
				: ''
		}
		${where}
		${group_by_fields.length > 0 ? `GROUP BY ${group_by_fields}` : ''}
		${fields.includes('date') ? 'ORDER BY toString(toYYYYMMDD(timestamp))' : ''}
	`

	const result = await client.query({
		query,
		format: 'JSONCompactEachRow',
		clickhouse_settings: { output_format_json_quote_64bit_integers: 0 }
	})
	const data = await result.json()

	res.json({ data })
})

app.get('/ping', (req, res) => {
	res.send('ok')
})

app.listen(port, () => {
	console.log(`Server running at port ${port}`)
})
