module.exports = {
	'magento.core_session': function(context, msg) {
		console.log("Session hit: id: " + msg.data.session_id + ", expires: " + new Date(msg.data.session_expires * 1000));
	},
	'magento.sales_flat_quote_item': function(context, msg) {
		console.log("Added to cart: " + msg.data.name);
	},
	'magento.log_url': function(context, msg) {
		console.log("Page hit on url " + msg.data.url_id + " by visitor " + msg.data.visitor_id + " at " +
			msg.data.visit_time);
	}
}