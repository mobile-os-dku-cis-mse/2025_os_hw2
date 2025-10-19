MAKEFLAGS += --silent

SRC	=	prod_cons_v2.c \

BUILD_DIR = build/

$(BUILD_DIR)%.o: %.c
	@mkdir -p $(@D)
	#@echo "  CC       $<      $@"
	@$(CC) $(CFLAGS) -c $< -o $@

OBJ	= 	$(SRC:%.c=$(BUILD_DIR)%.o)

NAME	=	PROD

CFLAGS 	= -I include/ -Wall -Wextra -g 

all:	$(NAME)

$(NAME):	$(OBJ)
		gcc -o $(NAME) $(SRC) $(CFLAGS) -pthread
	@ echo "PROD  compiled"

clean:
		rm -f $(OBJ)
	@ echo "clean done"

fclean: clean
		rm -f $(NAME)
	@ echo "fclean done"

re: 	fclean all

.PHONY: all $(NAME) clean fclean re .SILENT