"""
PDF Report Generator for indices analysis
"""
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from datetime import datetime
import logging
import os

logger = logging.getLogger(__name__)

class PDFReportGenerator:
    """
    Generate PDF reports for indices analysis
    """
    
    def __init__(self, output_path="reports"):
        """
        Initialize PDF generator
        
        Parameters:
        -----------
        output_path : str
            Directory to save PDF reports
        """
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)
    
    def create_report(self, analysis_df, ranking_df, strength_df, chart_image_path=None):
        """
        Create a comprehensive PDF report
        
        Parameters:
        -----------
        analysis_df : pd.DataFrame
            Main analysis results
        ranking_df : pd.DataFrame
            Performance ranking
        strength_df : pd.DataFrame
            Strength metrics
        chart_image_path : str
            Path to chart image to include
        
        Returns:
        --------
        str : Path to generated PDF
        """
        try:
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"indices_analysis_{timestamp}.pdf"
            filepath = os.path.join(self.output_path, filename)
            
            # Create PDF document
            doc = SimpleDocTemplate(
                filepath,
                pagesize=A4,
                rightMargin=0.5*inch,
                leftMargin=0.5*inch,
                topMargin=0.5*inch,
                bottomMargin=0.5*inch
            )
            
            # Container for PDF elements
            elements = []
            
            # Define styles
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                textColor=colors.HexColor('#003366'),
                spaceAfter=30,
                alignment=TA_CENTER
            )
            
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=14,
                textColor=colors.HexColor('#003366'),
                spaceAfter=12,
                spaceBefore=12
            )
            
            # Title Page
            title = Paragraph("Indian Stock Indices Analysis Report", title_style)
            elements.append(title)
            elements.append(Spacer(1, 0.2*inch))
            
            # Report metadata
            date_text = Paragraph(
                f"<b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                styles['Normal']
            )
            elements.append(date_text)
            elements.append(Spacer(1, 0.5*inch))
            
            # 1. Key Metrics Table
            elements.append(Paragraph("1. Key Metrics Summary", heading_style))
            
            metrics_data = [['Index', 'Current', 'Change %', 'Volatility', 'MA-20', 'MA-50']]
            for _, row in analysis_df.iterrows():
                metrics_data.append([
                    row['Index'],
                    str(row['Current Price']),
                    f"{row['Change (%)']}%",
                    f"{row['Volatility (%)']}%",
                    str(row['MA-20']),
                    str(row['MA-50'])
                ])
            
            metrics_table = Table(metrics_data, colWidths=[1.2*inch, 1*inch, 1*inch, 1*inch, 0.9*inch, 0.9*inch])
            metrics_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#003366')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            elements.append(metrics_table)
            elements.append(Spacer(1, 0.3*inch))
            
            # 2. Performance Ranking
            elements.append(Paragraph("2. Performance Ranking (Last 26 Weeks)", heading_style))
            
            ranking_data = [['Rank', 'Index', 'Change (%)', 'Volatility (%)']]
            for _, row in ranking_df.iterrows():
                ranking_data.append([
                    str(row['Rank']),
                    row['Index'],
                    f"{row['Change (%)']}%",
                    f"{row['Volatility (%)']}%"
                ])
            
            ranking_table = Table(ranking_data)
            ranking_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#006633')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            elements.append(ranking_table)
            elements.append(Spacer(1, 0.3*inch))
            
            # 3. Strength Score
            elements.append(Paragraph("3. Indices Strength Score (0-100)", heading_style))
            
            strength_data = [['Index', 'Performance %', 'Strength Score']]
            for _, row in strength_df.iterrows():
                strength_data.append([
                    row['Index'],
                    f"{row['Change (%)']}%",
                    str(row['Strength Score'])
                ])
            
            strength_table = Table(strength_data)
            strength_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#660033')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            elements.append(strength_table)
            
            # Add chart if provided
            if chart_image_path and os.path.exists(chart_image_path):
                elements.append(PageBreak())
                elements.append(Paragraph("4. Trend Analysis Chart", heading_style))
                elements.append(Spacer(1, 0.2*inch))
                
                try:
                    img = Image(chart_image_path, width=6*inch, height=4*inch)
                    elements.append(img)
                except Exception as e:
                    logger.warning(f"Could not embed chart image: {str(e)}")
            
            # Build PDF
            doc.build(elements)
            logger.info(f"PDF report generated successfully: {filepath}")
            return filepath
        
        except Exception as e:
            logger.error(f"Error creating PDF report: {str(e)}")
            return None
