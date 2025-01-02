package com.fnhackathon.MarketDataSimulator.Model;
import lombok.*;


@Data
public class TradeOrderDTO {
	private String ticker;
	private float bid_price;
	private float ask_price;
	private float last_trade_price;
	private int volume;
	private String timestamp;
}
